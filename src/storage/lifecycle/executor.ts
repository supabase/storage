import { randomUUID } from 'node:crypto'
import { hashStringToInt } from '@internal/hashing'
import { logger, logSchema } from '@internal/monitoring'
import {
  lifecycleArtifactOutcomes,
  lifecycleBytesDeleted,
  lifecycleDeleteMarkersDeleted,
  lifecycleObjectVersionsDeleted,
  lifecycleVersionsEligible,
  lifecycleVersionsExamined,
} from '@internal/monitoring/metrics'
import { getConfig } from '../../config'
import type { DeleteObjectDetailedResult } from '../backend'
import type { Database } from '../database'
import {
  type LifecycleArtifact,
  type LifecycleContinuation,
  type LifecycleShardClaimIdentity,
  type LifecycleShardCoordinate,
  type LifecycleShardState,
} from '../schemas'
import type { Storage } from '../storage'
import { compileLifecycleEvaluationRules } from './configuration'
import {
  advanceEmptyLifecyclePage,
  armLifecycleAttempt,
  createLifecycleContinuation,
  filterStagedLifecycleBatch,
  freezeLifecycleBatchVersion,
  recordLifecycleArtifactOutcomes,
  stageLifecycleBatch,
} from './continuation'

const configured = getConfig()
const NONCURRENT_METRIC_ATTRIBUTES = { scan_kind: 'NONCURRENT' }
export const LIFECYCLE_EVALUATION_INTERVAL_MS = 24 * 60 * 60 * 1000
const MAX_S3_KEY_BYTES = 1024
const MAX_IN_FLIGHT_RETRY_MS = 15 * 60 * 1000

export interface LifecycleExecutorOptions {
  pageSize: number
  jobBudgetMs: number
  claimLeaseMs: number
  recoveryGraceMs: number
  fairnessDelayMs: number
  now: () => Date
}

export interface LifecycleShardRunOptions {
  /** False permits armed-journal recovery but pauses new deletion work for this tenant. */
  tenantLifecycleEnabled: boolean
}

export interface LifecycleShardRunResult {
  status: 'NOT_CLAIMED' | 'COMPLETED' | 'PARTIAL' | 'PAUSED' | 'LOST_CLAIM'
  runId?: string
}

type StepResult = { continuation: LifecycleContinuation; result?: LifecycleShardRunResult }

const defaultOptions: LifecycleExecutorOptions = {
  // Arm and commit lock one advisory key per object name; keep batches near 100 locks.
  pageSize: 100,
  jobBudgetMs: configured.storageLifecycleJobBudgetMs,
  claimLeaseMs: configured.storageLifecycleClaimLeaseMs,
  recoveryGraceMs: configured.storageLifecycleRecoveryGraceMs,
  fairnessDelayMs: 5000,
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
  }

  async run(
    coordinate: LifecycleShardCoordinate,
    runOptions: LifecycleShardRunOptions
  ): Promise<LifecycleShardRunResult> {
    if (coordinate.scanKind !== 'NONCURRENT') {
      throw new Error(`Unsupported lifecycle scan kind ${coordinate.scanKind}`)
    }

    // Arming is safe only after the database also fences version promotion.
    if (!(await this.database.hasMigration('lifecycle-writer-coordination'))) {
      return { status: 'PAUSED' }
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
        const now = this.options.now()
        await this.database.releaseLifecycleShardClaim({
          ...identity,
          // A failed acknowledgement may follow a committed journal update.
          // Preserve the database continuation when releasing after an error.
          nextRunAt: retryAt(
            now,
            claimed?.failureCount ?? (error as { failureCount?: number }).failureCount ?? 0
          ),
          maxInFlightRetryAt: new Date(now.getTime() + MAX_IN_FLIGHT_RETRY_MS).toISOString(),
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
    identity: LifecycleShardClaimIdentity,
    runOptions: LifecycleShardRunOptions
  ): Promise<LifecycleShardRunResult> {
    const startedAt = this.options.now().getTime()
    let continuation = claimed.continuation

    // A closed tenant gate still finishes an armed journal so the promotion fence lifts.
    if (continuation?.batch?.inFlight) {
      const recovery = await this.recoverAttempt(identity, continuation)
      if (recovery.result) return recovery.result
      continuation = recovery.continuation
    }

    if (!runOptions.tenantLifecycleEnabled) {
      await this.releaseAfterDelay(identity, continuation)
      return { status: 'PAUSED', runId: continuation?.runId }
    }

    const bucket = await this.database.findLifecycleBucket(claimed.bucketId)
    if (
      bucket.versioning_status === 'DISABLED' ||
      !bucket.lifecycle_configuration ||
      !bucket.lifecycle_configuration_generation
    ) {
      await this.releaseIdle(identity)
      return { status: 'PAUSED', runId: continuation?.runId }
    }

    if (continuation?.generation !== bucket.lifecycle_configuration_generation) {
      continuation = null
    }

    if (!continuation) {
      continuation = createLifecycleContinuation({
        runId: randomUUID(),
        generation: bucket.lifecycle_configuration_generation,
        snapshotAt: this.options.now().toISOString(),
      })
    }

    const rules = compileLifecycleEvaluationRules(
      bucket.lifecycle_configuration,
      new Date(continuation.snapshotAt)
    )
    if (rules.length === 0) {
      await this.releaseIdle(identity)
      return { status: 'PAUSED', runId: continuation.runId }
    }

    while (this.options.now().getTime() - startedAt < this.options.jobBudgetMs) {
      if (continuation.batch) {
        const resumed = await this.processStagedBatch(identity, continuation)
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
      lifecycleVersionsExamined.add(page.rawRowsExamined, NONCURRENT_METRIC_ATTRIBUTES)
      lifecycleVersionsEligible.add(page.candidates.length, NONCURRENT_METRIC_ATTRIBUTES)

      if (page.candidates.length === 0) {
        continuation = advanceEmptyLifecyclePage(continuation, page)
      } else {
        continuation = stageLifecycleBatch(continuation, {
          pageEnd: page.pageEnd!,
          rawRowsExamined: page.rawRowsExamined,
          versions: page.candidates.map(freezeLifecycleBatchVersion),
        })
        const processed = await this.processStagedBatch(identity, continuation)
        if (processed.result) return processed.result
        continuation = processed.continuation
      }

      if (page.exhausted && !continuation.batch) {
        const completed = await this.completeRun(identity, continuation)
        if (!completed) return { status: 'LOST_CLAIM', runId: continuation.runId }
        return { status: 'COMPLETED', runId: continuation.runId }
      }
    }

    await this.releaseAfterDelay(identity, continuation)
    return { status: 'PARTIAL', runId: continuation.runId }
  }

  private async processStagedBatch(
    identity: LifecycleShardClaimIdentity,
    continuation: LifecycleContinuation
  ): Promise<StepResult> {
    const staged = continuation.batch!.versions
    const ready = await this.database.findLifecycleObjectVersions(identity.bucketId, staged)
    if (ready.length !== staged.length) {
      continuation = filterStagedLifecycleBatch(continuation, ready)
      if (!continuation.batch) return { continuation }
    }

    const armed = armLifecycleAttempt(continuation, {
      attemptId: randomUUID(),
      startedAt: this.options.now().toISOString(),
    })
    const state = await this.database.armLifecycleAttempt({
      ...identity,
      leaseMs: this.options.claimLeaseMs,
      continuation: armed,
    })
    if (!state) return this.resolveArmRejection(identity, continuation)
    return this.sendAttempt(identity, armed, false)
  }

  // The arm SQL clears the claim and continuation when a staged row loses eligibility.
  // Otherwise distinguish a changed policy from an unexpected rejection.
  private async resolveArmRejection(
    identity: LifecycleShardClaimIdentity,
    continuation: LifecycleContinuation
  ): Promise<StepResult> {
    const current = await this.database.revalidateLifecycleShardClaim(identity)
    if (!current) {
      return { continuation, result: { status: 'LOST_CLAIM', runId: continuation.runId } }
    }
    const bucket = await this.database.findLifecycleBucket(identity.bucketId)
    if (bucket.lifecycle_configuration_generation !== continuation.generation) {
      await this.releaseAfterDelay(identity, continuation)
      return { continuation, result: { status: 'PAUSED', runId: continuation.runId } }
    }

    throw new Error('Lifecycle arm fence rejected a staged batch with an unchanged claim')
  }

  private async recoverAttempt(
    identity: LifecycleShardClaimIdentity,
    continuation: LifecycleContinuation
  ): Promise<StepResult> {
    const attempt = continuation.batch!.inFlight!
    const recoverAt = new Date(attempt.startedAt).getTime() + this.options.recoveryGraceMs
    if (this.options.now().getTime() < recoverAt) {
      await this.database.releaseLifecycleShardClaim({
        ...identity,
        continuation,
        nextRunAt: new Date(recoverAt).toISOString(),
      })
      return { continuation, result: { status: 'PAUSED', runId: continuation.runId } }
    }

    // The armed journal authorizes an idempotent retry of every pending artifact.
    const state = await this.database.revalidateLifecycleRecoveryAttempt(
      identity,
      attempt.attemptId,
      this.options.claimLeaseMs
    )
    if (!state) {
      return { continuation, result: { status: 'LOST_CLAIM', runId: continuation.runId } }
    }
    return this.sendAttempt(identity, state.continuation!, true)
  }

  private async sendAttempt(
    identity: LifecycleShardClaimIdentity,
    continuation: LifecycleContinuation,
    recovering: boolean
  ): Promise<StepResult> {
    const attempt = continuation.batch!.inFlight!
    const pending = pendingArtifacts(continuation)
    const outcomes = new Map<string, Pick<LifecycleArtifact, 'outcome' | 'error'>>()
    const sendable: LifecycleArtifact[] = []
    const physicalKeys: string[] = []
    const isS3Backend = getConfig().storageBackendType === 's3'
    for (const artifact of pending) {
      const physicalKey = this.physicalKey(identity.bucketId, artifact.key)
      // A key beyond the S3 limit cannot exist, and the backend rejects it on every attempt.
      if (isS3Backend && Buffer.byteLength(physicalKey) > MAX_S3_KEY_BYTES) {
        outcomes.set(artifact.key, { outcome: 'ABSENT' })
      } else {
        sendable.push(artifact)
        physicalKeys.push(physicalKey)
      }
    }
    const results =
      physicalKeys.length === 0
        ? []
        : await this.storage.backend.deleteObjectsDetailed(
            this.storage.location.getRootLocation(),
            physicalKeys,
            {
              signal: AbortSignal.timeout(this.options.claimLeaseMs - this.options.recoveryGraceMs),
            }
          )
    correlateDeleteResults(sendable, results, outcomes, recovering)
    for (const outcome of ['DELETED', 'ABSENT', 'FAILED', 'UNRESOLVED'] as const) {
      const count = [...outcomes.values()].filter((artifact) => artifact.outcome === outcome).length
      if (count) lifecycleArtifactOutcomes.add(count, { ...NONCURRENT_METRIC_ATTRIBUTES, outcome })
    }
    const failed = [...outcomes.values()].filter(({ outcome }) => outcome === 'FAILED')
    if (failed.length > 0) {
      logSchema.warning(logger, '[Lifecycle] Backend rejected lifecycle deletions', {
        type: 'event',
        tenantId: this.database.tenantId,
        project: this.database.tenantId,
        metadata: JSON.stringify({
          bucketId: identity.bucketId,
          failedArtifacts: failed.length,
          error: failed[0].error,
        }),
      })
    }
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
    // These are acknowledged metadata commits, not a billing ledger. Lost commit
    // acknowledgements can undercount; recovery only reports its own new deletions.
    const before = continuation.counters
    const after = persisted.counters
    lifecycleObjectVersionsDeleted.add(
      after.objectVersionsDeleted - before.objectVersionsDeleted,
      NONCURRENT_METRIC_ATTRIBUTES
    )
    lifecycleDeleteMarkersDeleted.add(
      after.deleteMarkersDeleted - before.deleteMarkersDeleted,
      NONCURRENT_METRIC_ATTRIBUTES
    )
    lifecycleBytesDeleted.add(
      after.bytesDeleted - before.bytesDeleted,
      NONCURRENT_METRIC_ATTRIBUTES
    )
    if (persisted.batch?.inFlight) {
      const pending = pendingArtifacts(persisted)
      await this.database.releaseLifecycleShardClaim({
        ...identity,
        continuation: persisted,
        nextRunAt: retryAt(this.options.now(), state.failureCount, MAX_IN_FLIGHT_RETRY_MS),
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

  private async releaseIdle(identity: LifecycleShardClaimIdentity): Promise<void> {
    await this.database.releaseLifecycleShardClaim({
      ...identity,
      continuation: null,
      nextRunAt: null,
    })
  }

  private async releaseAfterDelay(
    identity: LifecycleShardClaimIdentity,
    continuation: LifecycleContinuation | null
  ): Promise<void> {
    await this.database.releaseLifecycleShardClaim({
      ...identity,
      continuation,
      nextRunAt: new Date(
        this.options.now().getTime() + this.options.fairnessDelayMs
      ).toISOString(),
    })
  }

  private physicalKey(bucketId: string, artifactKey: string): string {
    return this.storage.location.getKeyLocation({
      tenantId: this.database.tenantId,
      bucketId,
      objectName: artifactKey,
    })
  }

  private completeRun(identity: LifecycleShardClaimIdentity, continuation: LifecycleContinuation) {
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
        LIFECYCLE_EVALUATION_INTERVAL_MS,
        this.database.tenantId,
        identity
      )
    )
  }
}

function correlateDeleteResults(
  artifacts: LifecycleArtifact[],
  results: DeleteObjectDetailedResult[],
  outcomes: Map<string, Pick<LifecycleArtifact, 'outcome' | 'error'>>,
  recovering: boolean
): void {
  artifacts.forEach((artifact, index) => {
    const { outcome, error } = results[index]
    // A recovered request may already have deleted the key; only success resolves it.
    const unresolved =
      outcome === 'UNKNOWN' ||
      (recovering && outcome === 'FAILED' && artifact.outcome === 'UNRESOLVED')
    outcomes.set(artifact.key, {
      outcome: unresolved ? 'UNRESOLVED' : outcome,
      ...(error && { error: error.message || error.code || 'Backend delete failed' }),
    })
  })
}

function pendingArtifacts(continuation: LifecycleContinuation): LifecycleArtifact[] {
  return continuation.batch!.versions.flatMap((version) =>
    version.artifacts.filter(
      (artifact) => artifact.outcome === 'UNRESOLVED' || artifact.outcome === 'FAILED'
    )
  )
}

function lifecycleErrorSummary(error: unknown): Record<string, unknown> {
  return {
    name: error instanceof Error ? error.name : 'Error',
    message: error instanceof Error ? error.message.slice(0, 1000) : String(error).slice(0, 1000),
  }
}

function retryAt(now: Date, failureCount: number, maxDelayMs = 6 * 60 * 60 * 1000): string {
  const delay = Math.min(maxDelayMs, 5 * 60 * 1000 * 2 ** Math.min(failureCount, 10))
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
