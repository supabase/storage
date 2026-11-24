import { randomUUID } from 'node:crypto'
import { ErrorCode, isStorageError } from '@internal/errors'
import { hashStringToInt } from '@internal/hashing'
import { logger, logSchema } from '@internal/monitoring'
import {
  lifecycleBytesDeleted,
  lifecycleDeleteMarkersDeleted,
  lifecycleObjectVersionsDeleted,
  lifecycleVersionsEligible,
  lifecycleVersionsExamined,
} from '@internal/monitoring/metrics'
import { getConfig } from '../../config'
import type { Database } from '../database'
import { ObjectRemoved, type ObjectRemovedEvent } from '../events/webhooks/lifecycle-events'
import { logLifecycleEvent } from '../events/webhooks/lifecycle-log'
import {
  type LifecycleBatch,
  type LifecycleClaimIdentity,
  type LifecycleContinuation,
  type LifecycleDeletedMetadata,
  type LifecycleState,
} from '../schemas'
import { metadataSize } from '../schemas/object'
import type { Storage } from '../storage'
import { deleteLifecycleArtifacts } from './artifact-deletion'
import { compileLifecycleEvaluationRules } from './configuration'
import {
  advanceEmptyLifecyclePage,
  createLifecycleContinuation,
  freezeLifecycleBatchVersion,
} from './continuation'
import { lifecycleEvaluationBatches } from './evaluation'

const configured = getConfig()
export const LIFECYCLE_EVALUATION_INTERVAL_MS = 24 * 60 * 60 * 1000
const MAX_DELETION_RETRY_MS = 15 * 60 * 1000

export interface LifecycleExecutorOptions {
  pageSize: number
  deleteBatchSize: number
  jobBudgetMs: number
  claimLeaseMs: number
  fairnessDelayMs: number
  now: () => Date
}

export interface LifecycleRunOptions {
  // False pauses new deletion work for this tenant.
  tenantLifecycleEnabled: boolean
  signal?: AbortSignal
}

export interface LifecycleRunResult {
  status: 'NOT_CLAIMED' | 'COMPLETED' | 'PARTIAL' | 'PAUSED' | 'LOST_CLAIM' | 'INELIGIBLE'
  reason?: 'budget'
  runId?: string
}

const defaultOptions: LifecycleExecutorOptions = {
  pageSize: configured.storageLifecyclePageSize,
  deleteBatchSize: configured.storageLifecycleDeleteBatchSize,
  jobBudgetMs: configured.storageLifecycleJobBudgetMs,
  claimLeaseMs: configured.storageLifecycleClaimLeaseMs,
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

  async run(bucketId: string, runOptions: LifecycleRunOptions): Promise<LifecycleRunResult> {
    if (!(await this.database.hasMigration('lifecycle-writer-coordination'))) {
      return { status: 'PAUSED' }
    }

    const identity = { bucketId, claimId: randomUUID() }
    let claimed: LifecycleState | undefined
    try {
      // The claim can commit before its persisted continuation fails decoding.
      claimed = await this.database.claimLifecycleBucket({
        ...identity,
        leaseMs: this.options.claimLeaseMs,
      })
      if (!claimed) return { status: 'NOT_CLAIMED' }
      return await this.runClaimedBucket(claimed, identity, runOptions)
    } catch (error) {
      // The bucket delete cascaded the state row, so there is no claim to release.
      if (isStorageError(ErrorCode.NoSuchBucket, error)) return { status: 'LOST_CLAIM' }
      try {
        const now = this.options.now()
        await this.database.releaseLifecycleClaim({
          ...identity,
          // A failed acknowledgement may follow a committed arm.
          // Preserve the database continuation when releasing after an error.
          nextRunAt: retryAt(
            now,
            claimed?.failureCount ?? (error as { failureCount?: number }).failureCount ?? 0
          ),
          error: {
            name: error instanceof Error ? error.name : 'Error',
            message: lifecycleErrorMessage(error),
          },
        })
      } catch (releaseError) {
        logSchema.warning(logger, '[LifecycleRule] Failed to release lifecycle claim', {
          type: 'lifecycle-rule',
          tenantId: this.database.tenantId,
          project: this.database.tenantId,
          error: releaseError,
          metadata: JSON.stringify(identity),
        })
      }
      throw error
    }
  }

  private async runClaimedBucket(
    claimed: LifecycleState,
    identity: LifecycleClaimIdentity,
    runOptions: LifecycleRunOptions
  ): Promise<LifecycleRunResult> {
    const startedAt = this.options.now().getTime()
    // Only an armed batch renews the lease so the budget is held to the same bound.
    const budgetMs = Math.min(this.options.jobBudgetMs, leaseWorkMs(this.options.claimLeaseMs))
    let continuation = claimed.continuation

    if (!runOptions.tenantLifecycleEnabled) {
      const released = await this.releaseAfterDelay(identity, continuation)
      return { status: released ? 'PAUSED' : 'LOST_CLAIM', runId: continuation?.runId }
    }

    // The claim may have waited for a policy update while reading an older bucket snapshot.
    const bucket = await this.database.findLifecycleBucket(identity.bucketId)
    if (
      bucket.versioning_status === 'DISABLED' ||
      !bucket.lifecycle_configuration ||
      !bucket.lifecycle_configuration_generation
    ) {
      const released = await this.releaseIdle(identity)
      return { status: released ? 'PAUSED' : 'LOST_CLAIM', runId: continuation?.runId }
    }

    if (!continuation || continuation.generation !== bucket.lifecycle_configuration_generation) {
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
      const released = await this.releaseIdle(identity)
      return { status: released ? 'PAUSED' : 'LOST_CLAIM', runId: continuation.runId }
    }

    let evaluated = false
    while (
      (!evaluated || this.options.now().getTime() - startedAt < budgetMs) &&
      !runOptions.signal?.aborted
    ) {
      evaluated = true
      const page = await this.database.evaluateNoncurrentLifecyclePage({
        bucketId: claimed.bucketId,
        snapshotAt: continuation.snapshotAt,
        cursor: continuation.cursor,
        rules,
        pageSize: this.options.pageSize,
      })
      lifecycleVersionsExamined.add(page.rawRowsExamined)
      lifecycleVersionsEligible.add(page.candidates.length)

      if (page.candidates.length === 0) {
        continuation = advanceEmptyLifecyclePage(continuation, page)
      } else {
        let first = true
        for (const batch of lifecycleEvaluationBatches(page, this.options.deleteBatchSize)) {
          // Allow one chunk after evaluation so a slow scan can still make progress.
          if (
            runOptions.signal?.aborted ||
            (!first && this.options.now().getTime() - startedAt >= budgetMs)
          ) {
            const budgetExhausted = !runOptions.signal?.aborted
            const released = await this.releaseAfterDelay(identity, continuation)
            return {
              status: released ? 'PARTIAL' : 'LOST_CLAIM',
              ...(released && budgetExhausted ? { reason: 'budget' as const } : {}),
              runId: continuation.runId,
            }
          }
          first = false
          const processed = await this.deleteBatch(
            identity,
            continuation,
            {
              pageEnd: batch.pageEnd!,
              rawRowsExamined: batch.rawRowsExamined,
              versions: batch.candidates.map(freezeLifecycleBatchVersion),
            },
            runOptions.signal
          )
          if ('status' in processed) return processed
          continuation = processed
        }
      }

      if (page.exhausted) {
        const completed = await this.completeRun(identity, continuation)
        return { status: completed ? 'COMPLETED' : 'LOST_CLAIM', runId: continuation.runId }
      }
    }

    const budgetExhausted = !runOptions.signal?.aborted
    const released = await this.releaseAfterDelay(identity, continuation)
    return {
      status: released ? 'PARTIAL' : 'LOST_CLAIM',
      ...(released && budgetExhausted ? { reason: 'budget' as const } : {}),
      runId: continuation.runId,
    }
  }

  private async deleteBatch(
    identity: LifecycleClaimIdentity,
    continuation: LifecycleContinuation,
    batch: LifecycleBatch,
    signal?: AbortSignal
  ): Promise<LifecycleContinuation | LifecycleRunResult> {
    const result = await this.database.armLifecycleAttempt({
      ...identity,
      leaseMs: this.options.claimLeaseMs,
      continuation,
      batch,
    })
    if (result.status === 'fenced') return this.resolveArmRejection(identity, continuation)
    if (result.status === 'ineligible') return { status: 'INELIGIBLE', runId: continuation.runId }
    const armed = result.state.continuation!
    const before = continuation.counters
    const after = armed.counters
    lifecycleObjectVersionsDeleted.add(after.objectVersionsDeleted - before.objectVersionsDeleted)
    lifecycleDeleteMarkersDeleted.add(after.deleteMarkersDeleted - before.deleteMarkersDeleted)
    lifecycleBytesDeleted.add(after.bytesDeleted - before.bytesDeleted)

    this.logDeleted(identity.bucketId, result.deleted)

    if (result.artifacts.length === 0) return armed
    if (signal?.aborted) {
      const released = await this.releaseAfterDelay(identity, armed)
      return { status: released ? 'PARTIAL' : 'LOST_CLAIM', runId: armed.runId }
    }
    const leaseTimeout = AbortSignal.timeout(leaseWorkMs(this.options.claimLeaseMs))
    const { failed, error } = await deleteLifecycleArtifacts({
      storage: this.storage,
      database: this.database,
      artifacts: result.artifacts,
      signal: signal ? AbortSignal.any([signal, leaseTimeout]) : leaseTimeout,
    })
    if (failed === 0) return armed
    if (signal?.aborted) {
      const released = await this.releaseAfterDelay(identity, armed)
      return { status: released ? 'PARTIAL' : 'LOST_CLAIM', runId: armed.runId }
    }

    logSchema.warning(logger, '[LifecycleRule] Backend rejected lifecycle deletions', {
      type: 'lifecycle-rule',
      tenantId: this.database.tenantId,
      project: this.database.tenantId,
      metadata: JSON.stringify({
        bucketId: identity.bucketId,
        failedArtifacts: failed,
        error: error && lifecycleErrorMessage(error),
      }),
    })
    const released = await this.database.releaseLifecycleClaim({
      ...identity,
      nextRunAt: retryAt(this.options.now(), result.state.failureCount, MAX_DELETION_RETRY_MS),
      error: {
        name: 'LifecycleDeletionIncomplete',
        message: lifecycleErrorMessage(error ?? 'Lifecycle deletion remains incomplete'),
        pendingArtifacts: failed,
      },
    })
    return { status: released ? 'PARTIAL' : 'LOST_CLAIM', runId: armed.runId }
  }

  // Usage log only: lifecycle sends no webhook or CDN purge.
  private logDeleted(bucketId: string, deleted: LifecycleDeletedMetadata[]) {
    const applyTime = Date.now()
    for (const { name, version, isDeleteMarker, metadata } of deleted) {
      const payload: ObjectRemovedEvent = {
        tenant: this.database.tenant(),
        name,
        bucketId,
        version,
        metadata,
        bytesChanged: deletedBytesChanged(isDeleteMarker, metadata),
        reqId: this.database.reqId,
        sbReqId: this.database.sbReqId,
      }
      logLifecycleEvent({
        eventType: ObjectRemoved.eventType,
        version: ObjectRemoved.version,
        applyTime,
        payload,
      })
    }
  }

  private async resolveArmRejection(
    identity: LifecycleClaimIdentity,
    continuation: LifecycleContinuation
  ): Promise<LifecycleRunResult> {
    const current = await this.database.revalidateLifecycleClaim(identity)
    if (!current) return { status: 'LOST_CLAIM', runId: continuation.runId }
    const bucket = await this.database.findLifecycleBucket(identity.bucketId)
    if (bucket.lifecycle_configuration_generation !== continuation.generation) {
      const released = await this.releaseAfterDelay(identity, null)
      return { status: released ? 'PAUSED' : 'LOST_CLAIM', runId: continuation.runId }
    }

    throw new Error('Lifecycle arm fence rejected a batch with an unchanged claim')
  }

  private async releaseIdle(identity: LifecycleClaimIdentity): Promise<boolean> {
    return this.database.releaseLifecycleClaim({
      ...identity,
      continuation: null,
      nextRunAt: null,
    })
  }

  private async releaseAfterDelay(
    identity: LifecycleClaimIdentity,
    continuation: LifecycleContinuation | null
  ): Promise<boolean> {
    return this.database.releaseLifecycleClaim({
      ...identity,
      continuation,
      nextRunAt: new Date(
        this.options.now().getTime() + this.options.fairnessDelayMs
      ).toISOString(),
    })
  }

  private completeRun(identity: LifecycleClaimIdentity, continuation: LifecycleContinuation) {
    return this.database.completeLifecycleRun(
      { ...identity, generation: continuation.generation },
      {
        runId: continuation.runId,
        generation: continuation.generation,
        snapshotAt: continuation.snapshotAt,
        counters: continuation.counters,
        completedAt: this.options.now().toISOString(),
      },
      nextEvaluationAt(this.options.now(), this.database.tenantId, identity.bucketId)
    )
  }
}

export function lifecycleErrorMessage(error: unknown): string {
  const value = error instanceof Error ? error.message : String(error)
  return value.slice(0, 1000).toWellFormed()
}

// Work stops short of the lease so the outcome is recorded and the claim released
// before the lease expires and the bucket is dispatched again.
function leaseWorkMs(leaseMs: number): number {
  return leaseMs - Math.min(30_000, Math.floor(leaseMs / 2))
}

function retryAt(now: Date, failureCount: number, maxDelayMs = 6 * 60 * 60 * 1000): string {
  const delay = Math.min(maxDelayMs, 5 * 60 * 1000 * 2 ** failureCount)
  return new Date(now.getTime() + delay).toISOString()
}

export function evaluationJitterMs(
  key: string,
  windowMs = LIFECYCLE_EVALUATION_INTERVAL_MS / 10
): number {
  const hash = Math.abs(hashStringToInt(key))
  if (windowMs > LIFECYCLE_EVALUATION_INTERVAL_MS) {
    return (hash % Math.floor(windowMs / 1000)) * 1000
  }
  return hash % windowMs
}

function deletedBytesChanged(
  isDeleteMarker: boolean,
  metadata: Record<string, unknown> | null
): number {
  if (isDeleteMarker) return 0
  const size = metadataSize(metadata)
  return size > 0 ? -size : 0
}

function nextEvaluationAt(now: Date, tenantId: string, bucketId: string): string {
  const jitter = evaluationJitterMs(`${tenantId}/${bucketId}`)
  return new Date(now.getTime() + LIFECYCLE_EVALUATION_INTERVAL_MS + jitter).toISOString()
}
