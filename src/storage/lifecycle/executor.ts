import { randomUUID } from 'node:crypto'
import { hashStringToInt } from '@internal/hashing'
import { logger, logSchema } from '@internal/monitoring'
import { lifecycleVersionsEligible, lifecycleVersionsExamined } from '@internal/monitoring/metrics'
import { getConfig } from '../../config'
import { type DeleteObjectDetailedResult, isMissingBackendObject } from '../backend'
import type { Database } from '../database'
import { decodeCandidate } from '../database/lifecycle'
import type {
  LifecycleArtifact,
  LifecycleBatchVersion,
  LifecycleCandidate,
  LifecycleContinuation,
  LifecycleShardCoordinate,
  LifecycleShardState,
} from '../schemas'
import type { Storage } from '../storage'
import { compileLifecycleEvaluationRules } from './configuration'
import {
  advanceEmptyLifecyclePage,
  armLifecycleAttempt,
  createLifecycleContinuation,
  filterStagedLifecycleBatch,
  freezeLifecycleBatchVersion,
  lifecycleVersionIdentity,
  recordLifecycleArtifactOutcomes,
  refreshStagedLifecycleBatchArtifacts,
  stageLifecycleBatch,
} from './continuation'

const configured = getConfig()
const RECOVERY_HEAD_CONCURRENCY = 8

export interface LifecycleExecutorOptions {
  pageSize: number
  jobBudgetMs: number
  claimLeaseMs: number
  recoveryGraceMs: number
  fairnessDelayMs: number
  evaluationIntervalMs: number
  deleteEnabled: boolean
  now: () => Date
}

export interface LifecycleShardRunOptions {
  /** The tenant's objectVersioning feature; false permits only armed recovery. */
  tenantVersioningEnabled: boolean
}

export interface LifecycleShardRunResult {
  status: 'NOT_CLAIMED' | 'COMPLETED' | 'PARTIAL' | 'PAUSED' | 'LOST_CLAIM'
  runId?: string
}

type ClaimIdentity = LifecycleShardCoordinate & { claimId: string }
type StepResult = { continuation: LifecycleContinuation; result?: LifecycleShardRunResult }

const defaultOptions: LifecycleExecutorOptions = {
  pageSize: configured.storageLifecyclePageSize,
  jobBudgetMs: configured.storageLifecycleJobBudgetMs,
  claimLeaseMs: configured.storageLifecycleClaimLeaseMs,
  recoveryGraceMs: configured.storageLifecycleRecoveryGraceMs,
  fairnessDelayMs: configured.storageLifecycleFairnessDelayMs,
  evaluationIntervalMs: configured.storageLifecycleEvaluationIntervalMs,
  // Lifecycle deletion is S3-only during staging; versioning also supports file storage.
  deleteEnabled: configured.versioningEnabled && configured.storageBackendType === 's3',
  now: () => new Date(),
}

export class NoncurrentLifecycleExecutor {
  private readonly database: Database
  private readonly options: LifecycleExecutorOptions

  constructor(
    private readonly storage: Storage,
    options: Partial<LifecycleExecutorOptions> = {}
  ) {
    this.database = storage.db.asSuperUser()
    this.options = { ...defaultOptions, ...options }
    validateExecutorOptions(this.options)
  }

  async run(
    coordinate: LifecycleShardCoordinate,
    runOptions: LifecycleShardRunOptions
  ): Promise<LifecycleShardRunResult> {
    if (coordinate.scanKind !== 'NONCURRENT') {
      throw new Error(`Unsupported lifecycle scan kind ${coordinate.scanKind}`)
    }

    const identity = { ...coordinate, claimId: randomUUID() }
    let claimed: LifecycleShardState | undefined
    try {
      // The claim can commit before its persisted continuation fails decoding.
      claimed = await this.database.claimLifecycleShard({
        ...identity,
        leaseMs: this.options.claimLeaseMs,
      })
      if (!claimed) return { status: 'NOT_CLAIMED' }
      return await this.runClaimedShard(claimed, identity, runOptions)
    } catch (error) {
      try {
        await this.database.releaseLifecycleShardClaim({
          ...identity,
          // A failed acknowledgement may follow a committed journal update.
          // Preserve the database continuation when releasing after an error.
          nextRunAt: retryAt(this.options.now(), claimed?.failureCount ?? 0),
          error: lifecycleErrorSummary(error),
        })
      } catch (releaseError) {
        logSchema.warning(logger, '[Lifecycle] Failed to release lifecycle claim', {
          type: 'event',
          tenantId: this.database.tenantId,
          project: this.database.tenantId,
          error: releaseError,
          metadata: JSON.stringify(identity),
        })
      }
      throw error
    }
  }

  private async runClaimedShard(
    claimed: LifecycleShardState,
    identity: ClaimIdentity,
    runOptions: LifecycleShardRunOptions
  ): Promise<LifecycleShardRunResult> {
    const startedAt = this.options.now().getTime()
    let continuation = claimed.continuation
    if (continuation?.batch?.inFlight) {
      const recovery = await this.recoverAttempt(identity, continuation, runOptions)
      if (recovery.result) return recovery.result
      continuation = recovery.continuation
    }

    if (!this.options.deleteEnabled || !runOptions.tenantVersioningEnabled) {
      await this.releaseAfterDelay(identity, continuation, this.options.fairnessDelayMs)
      return { status: 'PAUSED', runId: continuation?.runId }
    }

    const bucket = await this.database.findLifecycleBucket(claimed.bucketId)
    if (!bucket.lifecycle_configuration || !bucket.lifecycle_configuration_generation) {
      await this.database.releaseLifecycleShardClaim({
        ...identity,
        continuation: null,
        nextRunAt: null,
      })
      return { status: 'PAUSED', runId: continuation?.runId }
    }

    if (continuation?.generation !== bucket.lifecycle_configuration_generation) {
      continuation = null
    }

    if (!continuation) {
      continuation = createLifecycleContinuation({
        runId: randomUUID(),
        trigger: 'scheduled',
        generation: bucket.lifecycle_configuration_generation,
        snapshotAt: this.options.now().toISOString(),
        topology: {
          scanKind: 'NONCURRENT',
          epoch: claimed.shardEpoch,
          shardId: claimed.shardId,
          shardCount: claimed.shardCount,
        },
      })
      if (!(await this.database.saveLifecycleContinuation(identity, continuation))) {
        return { status: 'LOST_CLAIM', runId: continuation.runId }
      }
    }

    const rules = compileLifecycleEvaluationRules(
      bucket.lifecycle_configuration,
      new Date(continuation.snapshotAt)
    )
    if (rules.length === 0) {
      await this.database.releaseLifecycleShardClaim({
        ...identity,
        continuation: null,
        nextRunAt: null,
      })
      return { status: 'PAUSED', runId: continuation.runId }
    }

    while (this.options.now().getTime() - startedAt < this.options.jobBudgetMs) {
      if (continuation.batch) {
        const resumed = await this.processStagedBatch(identity, claimed, continuation, runOptions)
        if (resumed.result) return resumed.result
        continuation = resumed.continuation
        continue
      }

      const page = await this.database.evaluateNoncurrentLifecyclePage({
        bucketId: claimed.bucketId,
        snapshotAt: continuation.snapshotAt,
        cursor: continuation.cursor,
        rules,
        pageSize: this.options.pageSize,
      })
      const metricAttributes = { scan_kind: 'NONCURRENT' }
      lifecycleVersionsExamined.add(page.rawRowsExamined, metricAttributes)
      lifecycleVersionsEligible.add(page.candidates.length, metricAttributes)

      if (page.candidates.length === 0) {
        continuation = advanceEmptyLifecyclePage(continuation, page)
        if (!(await this.database.saveLifecycleContinuation(identity, continuation))) {
          return { status: 'LOST_CLAIM', runId: continuation.runId }
        }
      } else {
        continuation = stageLifecycleBatch(continuation, {
          pageEnd: page.pageEnd!,
          rawRowsExamined: page.rawRowsExamined,
          versions: page.candidates.map(freezeLifecycleBatchVersion),
        })
        if (!(await this.database.saveLifecycleContinuation(identity, continuation))) {
          return { status: 'LOST_CLAIM', runId: continuation.runId }
        }
        const processed = await this.processStagedBatch(identity, claimed, continuation, runOptions)
        if (processed.result) return processed.result
        continuation = processed.continuation
      }

      if (page.exhausted && !continuation.batch) {
        const completed = await this.completeRun(identity, continuation)
        if (!completed) return { status: 'LOST_CLAIM', runId: continuation.runId }
        return { status: 'COMPLETED', runId: continuation.runId }
      }
    }

    await this.releaseAfterDelay(identity, continuation, this.options.fairnessDelayMs)
    return { status: 'PARTIAL', runId: continuation.runId }
  }

  private async processStagedBatch(
    identity: ClaimIdentity,
    claimed: LifecycleShardState,
    continuation: LifecycleContinuation,
    runOptions: LifecycleShardRunOptions
  ): Promise<StepResult> {
    if (!continuation.batch) throw new Error('Lifecycle batch is missing')
    if (continuation.batch.inFlight) {
      return this.recoverAttempt(identity, continuation, runOptions)
    }
    if (
      continuation.generation !== claimed.configurationGeneration ||
      !this.options.deleteEnabled ||
      !runOptions.tenantVersioningEnabled
    ) {
      return this.pause(identity, continuation)
    }

    const ready = await this.reloadStagedCandidates(identity.bucketId, continuation.batch.versions)
    if (ready.length !== continuation.batch.versions.length) {
      continuation = filterStagedLifecycleBatch(continuation, ready)
      if (!(await this.database.saveLifecycleContinuation(identity, continuation))) {
        return { continuation, result: { status: 'LOST_CLAIM', runId: continuation.runId } }
      }
      if (!continuation.batch) return { continuation }
    }

    const armed = armLifecycleAttempt(refreshStagedLifecycleBatchArtifacts(continuation, ready), {
      attemptId: randomUUID(),
      authorizedGeneration: continuation.generation,
      startedAt: this.options.now().toISOString(),
    })
    const state = await this.database.armLifecycleAttempt({
      ...identity,
      configurationGeneration: continuation.generation,
      leaseMs: this.options.claimLeaseMs,
      continuation: armed,
    })
    if (!state) return this.resolveArmRejection(identity, continuation)
    return this.sendAttempt(identity, armed)
  }

  // The arm SQL clears the claim and continuation when a staged row loses eligibility.
  // Otherwise distinguish a changed policy from an unexpected rejection.
  private async resolveArmRejection(
    identity: ClaimIdentity,
    continuation: LifecycleContinuation
  ): Promise<StepResult> {
    const current = await this.database.revalidateLifecycleShardClaim(identity)
    if (!current) {
      return { continuation, result: { status: 'LOST_CLAIM', runId: continuation.runId } }
    }
    if (current.configurationGeneration !== continuation.generation) {
      return this.pause(identity, continuation)
    }

    throw new Error('Lifecycle arm fence rejected a staged batch with an unchanged claim')
  }

  private async recoverAttempt(
    identity: ClaimIdentity,
    continuation: LifecycleContinuation,
    runOptions: LifecycleShardRunOptions
  ): Promise<StepResult> {
    const attempt = continuation.batch?.inFlight
    if (!attempt) return { continuation }
    const recoverAt = new Date(attempt.startedAt).getTime() + this.options.recoveryGraceMs
    if (this.options.now().getTime() < recoverAt) {
      await this.database.releaseLifecycleShardClaim({
        ...identity,
        continuation,
        nextRunAt: new Date(recoverAt).toISOString(),
      })
      return { continuation, result: { status: 'PAUSED', runId: continuation.runId } }
    }

    const canRedrive = this.options.deleteEnabled && runOptions.tenantVersioningEnabled
    if (canRedrive) {
      // The armed journal authorizes an idempotent retry of every pending artifact.
      const state = await this.database.revalidateLifecycleRecoveryAttempt(
        identity,
        attempt.attemptId,
        this.options.claimLeaseMs
      )
      if (!state) {
        return { continuation, result: { status: 'LOST_CLAIM', runId: continuation.runId } }
      }
      return this.sendAttempt(identity, state.continuation!)
    }

    const startedAt = this.options.now().getTime()
    const unresolved = continuation.batch!.versions.flatMap((version) =>
      version.artifacts.filter((artifact) => artifact.outcome === 'UNRESOLVED')
    )
    let persisted = continuation
    // Save confirmed absence between bounded waves. The explicit deadline also
    // bounds backend retries and the GET used to confirm ambiguous HEAD responses.
    for (
      let offset = 0;
      offset < unresolved.length || offset === 0;
      offset += RECOVERY_HEAD_CONCURRENCY
    ) {
      const current = await this.database.revalidateLifecycleRecoveryAttempt(
        identity,
        attempt.attemptId,
        this.options.claimLeaseMs
      )
      if (!current) {
        return { continuation: persisted, result: { status: 'LOST_CLAIM', runId: persisted.runId } }
      }
      const inspected = await this.inspectUnresolvedArtifacts(
        identity.bucketId,
        current.continuation!,
        unresolved.slice(offset, offset + RECOVERY_HEAD_CONCURRENCY),
        Math.max(
          1,
          Math.min(
            this.options.jobBudgetMs - (this.options.now().getTime() - startedAt),
            Math.floor(this.options.claimLeaseMs / 2),
            2 ** 31 - 1
          )
        )
      )
      const state = await this.database.commitLifecycleAttempt({
        ...identity,
        attemptId: attempt.attemptId,
        continuation: inspected,
      })
      if (!state) {
        return { continuation: persisted, result: { status: 'LOST_CLAIM', runId: persisted.runId } }
      }
      persisted = state.continuation!
      if (!persisted.batch?.inFlight) return { continuation: persisted }
      if (this.options.now().getTime() - startedAt >= this.options.jobBudgetMs) break
    }

    return this.pause(identity, persisted)
  }

  private async sendAttempt(
    identity: ClaimIdentity,
    continuation: LifecycleContinuation
  ): Promise<StepResult> {
    const batch = continuation.batch
    const attempt = batch?.inFlight
    if (!attempt) throw new Error('Lifecycle attempt is not armed')
    const pending = batch.versions.flatMap((version) =>
      version.artifacts.filter(
        (artifact) => artifact.outcome === 'UNRESOLVED' || artifact.outcome === 'FAILED'
      )
    )
    const physicalKeys = pending.map((artifact) =>
      this.physicalKey(identity.bucketId, artifact.key)
    )
    const results =
      physicalKeys.length === 0
        ? []
        : await this.storage.backend.deleteObjectsDetailed(
            this.storage.location.getRootLocation(),
            physicalKeys
          )
    const outcomes = correlateDeleteResults(pending, physicalKeys, results)
    const recorded = recordLifecycleArtifactOutcomes(continuation, outcomes)
    const state = await this.database.commitLifecycleAttempt({
      ...identity,
      attemptId: attempt.attemptId,
      continuation: recorded,
    })
    if (!state) {
      return { continuation, result: { status: 'LOST_CLAIM', runId: continuation.runId } }
    }
    const persisted = state.continuation!
    if (persisted.batch?.inFlight) {
      const pending = persisted.batch.versions.flatMap((version) =>
        version.artifacts.filter(
          (artifact) => artifact.outcome === 'UNRESOLVED' || artifact.outcome === 'FAILED'
        )
      )
      await this.database.releaseLifecycleShardClaim({
        ...identity,
        continuation: persisted,
        nextRunAt: retryAt(this.options.now(), state.failureCount),
        error: {
          name: 'LifecycleDeletionIncomplete',
          message:
            pending.find((artifact) => artifact.error)?.error?.slice(0, 1000) ??
            'Lifecycle deletion remains incomplete',
          pendingArtifacts: pending.length,
        },
      })
      return { continuation: persisted, result: { status: 'PARTIAL', runId: persisted.runId } }
    }
    return { continuation: persisted }
  }

  private async inspectUnresolvedArtifacts(
    bucketId: string,
    continuation: LifecycleContinuation,
    unresolved: LifecycleArtifact[],
    budgetMs: number
  ): Promise<LifecycleContinuation> {
    if (unresolved.length === 0) return continuation

    const signal = AbortSignal.timeout(budgetMs)
    const outcomes = new Map<string, { outcome: 'ABSENT' | 'UNRESOLVED'; error?: string }>()
    await Promise.all(
      unresolved.map(async (artifact) => {
        try {
          await this.storage.backend.headObject(
            this.storage.location.getRootLocation(),
            this.physicalKey(bucketId, artifact.key),
            undefined,
            { confirmMissing: true, signal }
          )
        } catch (error) {
          outcomes.set(
            artifact.key,
            isMissingBackendObject(error)
              ? { outcome: 'ABSENT' }
              : {
                  outcome: 'UNRESOLVED',
                  error: error instanceof Error ? error.message : String(error),
                }
          )
        }
      })
    )
    return outcomes.size === 0
      ? continuation
      : recordLifecycleArtifactOutcomes(continuation, outcomes)
  }

  private async reloadStagedCandidates(
    bucketId: string,
    versions: LifecycleBatchVersion[]
  ): Promise<LifecycleCandidate[]> {
    const rows = await this.database.findLifecycleObjectVersions(
      bucketId,
      versions.map(({ name, version }) => ({ name, version }))
    )
    const byIdentity = new Map(
      rows.map((row) => {
        if (row.version === undefined) {
          throw new Error('A staged lifecycle candidate has no physical version identity')
        }
        return [lifecycleVersionIdentity(row.name, row.version), row]
      })
    )
    return versions.flatMap((version) => {
      const row = byIdentity.get(lifecycleVersionIdentity(version.name, version.version))
      if (!row || row.archived_at === null) return []
      try {
        return [
          decodeCandidate({
            id: row.id,
            bucketId: row.bucket_id,
            name: row.name,
            version: row.version,
            isVersioned: row.is_versioned,
            isDeleteMarker: row.is_delete_marker,
            metadata: row.metadata ?? null,
            createdAt: row.created_at,
            archivedAt: row.archived_at,
          }),
        ]
      } catch {
        throw new Error('A staged lifecycle candidate no longer exists or is malformed')
      }
    })
  }

  private async pause(
    identity: ClaimIdentity,
    continuation: LifecycleContinuation
  ): Promise<StepResult> {
    await this.releaseAfterDelay(identity, continuation, this.options.fairnessDelayMs)
    return { continuation, result: { status: 'PAUSED', runId: continuation.runId } }
  }

  private releaseAfterDelay(
    identity: ClaimIdentity,
    continuation: LifecycleContinuation | null,
    delayMs: number
  ): Promise<boolean> {
    return this.database.releaseLifecycleShardClaim({
      ...identity,
      continuation,
      nextRunAt: new Date(this.options.now().getTime() + delayMs).toISOString(),
    })
  }

  private physicalKey(bucketId: string, artifactKey: string): string {
    return this.storage.location.getKeyLocation({
      tenantId: this.database.tenantId,
      bucketId,
      objectName: artifactKey,
    })
  }

  private completeRun(identity: ClaimIdentity, continuation: LifecycleContinuation) {
    return this.database.completeLifecycleShardRun(
      identity,
      {
        runId: continuation.runId,
        generation: continuation.generation,
        snapshotAt: continuation.snapshotAt,
        counters: continuation.counters,
        completedAt: this.options.now().toISOString(),
      },
      nextEvaluationAt(
        this.options.now(),
        this.options.evaluationIntervalMs,
        this.database.tenantId,
        identity
      )
    )
  }
}

function correlateDeleteResults(
  artifacts: Array<{ key: string }>,
  physicalKeys: string[],
  results: DeleteObjectDetailedResult[]
): Map<string, { outcome: 'DELETED' | 'FAILED' | 'UNRESOLVED'; error?: string }> {
  const byKey = new Map<string, DeleteObjectDetailedResult[]>()
  for (const result of results) {
    const entries = byKey.get(result.key) ?? []
    entries.push(result)
    byKey.set(result.key, entries)
  }

  const outcomes = new Map<
    string,
    { outcome: 'DELETED' | 'FAILED' | 'UNRESOLVED'; error?: string }
  >()
  artifacts.forEach((artifact, index) => {
    const physicalKey = physicalKeys[index]
    const matching = byKey.get(physicalKey) ?? []
    const result = matching.shift()
    if (matching.length === 0) byKey.delete(physicalKey)
    if (!result) {
      outcomes.set(artifact.key, {
        outcome: 'UNRESOLVED',
        error: 'Backend delete response omitted this artifact',
      })
      return
    }
    outcomes.set(artifact.key, {
      outcome:
        result.outcome === 'DELETED'
          ? 'DELETED'
          : result.outcome === 'FAILED'
            ? 'FAILED'
            : 'UNRESOLVED',
      ...(result.error?.message === undefined ? {} : { error: result.error.message }),
    })
  })

  if (byKey.size > 0) {
    for (const artifact of artifacts) {
      const current = outcomes.get(artifact.key)
      if (current?.outcome === 'DELETED') {
        outcomes.set(artifact.key, {
          outcome: 'UNRESOLVED',
          error: 'Backend delete response contained unexpected or duplicate results',
        })
      }
    }
  }
  return outcomes
}

function validateExecutorOptions(options: LifecycleExecutorOptions) {
  const positive = [
    options.pageSize,
    options.jobBudgetMs,
    options.claimLeaseMs,
    options.recoveryGraceMs,
    options.fairnessDelayMs,
    options.evaluationIntervalMs,
  ]
  if (positive.some((value) => !Number.isSafeInteger(value) || value < 1)) {
    throw new Error('Lifecycle executor limits must be positive integers')
  }
  if (options.pageSize > 500) throw new Error('Lifecycle executor page size cannot exceed 500')
  if (options.jobBudgetMs >= options.claimLeaseMs) {
    throw new Error('Lifecycle executor job budget must be shorter than its claim lease')
  }
}

function lifecycleErrorSummary(error: unknown): Record<string, unknown> {
  return {
    name: error instanceof Error ? error.name : 'Error',
    message: error instanceof Error ? error.message.slice(0, 1000) : String(error).slice(0, 1000),
  }
}

function retryAt(now: Date, failureCount: number): string {
  const delay = Math.min(6 * 60 * 60 * 1000, 5 * 60 * 1000 * 2 ** Math.min(failureCount, 10))
  return new Date(now.getTime() + delay).toISOString()
}

function nextEvaluationAt(
  now: Date,
  intervalMs: number,
  tenantId: string,
  coordinate: LifecycleShardCoordinate
): string {
  const jitterWindow = Math.max(1, Math.floor(intervalMs / 10))
  const hash = hashStringToInt(
    `${tenantId}/${coordinate.bucketId}/${coordinate.scanKind}/${coordinate.shardEpoch}/${coordinate.shardId}`
  )
  const jitter = Math.abs(hash) % jitterWindow
  return new Date(now.getTime() + intervalMs + jitter).toISOString()
}
