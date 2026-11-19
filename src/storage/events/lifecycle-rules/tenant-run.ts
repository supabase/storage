import { LifecycleTenantStorePg, multitenantPgExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload, Queue as EventQueue } from '@internal/queue'
import { JobWithMetadata, Queue, SendOptions, WorkOptions } from 'pg-boss'
import { getConfig } from '../../../config'
import { retryAbandonedLifecycleArtifacts } from '../../lifecycle/abandoned-artifacts'
import {
  evaluationJitterMs,
  LIFECYCLE_EVALUATION_INTERVAL_MS,
  lifecycleErrorMessage,
} from '../../lifecycle/executor'
import { isLifecycleEnabledForTenant } from '../../lifecycle/guards'
import type { LifecycleDispatchState } from '../../schemas'
import { BaseEvent } from '../base-event'
import { LifecycleBucketRun } from './bucket-run'

const LIFECYCLE_TENANT_FANOUT = 8
const LIFECYCLE_TENANT_CONCURRENCY = 4
const LIFECYCLE_TENANT_EXPIRE_MINUTES = 3
const LIFECYCLE_FEATURE_CACHE_RETRY_MS = 5 * 60_000
const LIFECYCLE_ABANDONED_RETRY_INTERVAL_MS = 60 * 60_000
const LIFECYCLE_ABANDONED_RETRY_BUDGET_MS = 60_000
export const LIFECYCLE_TENANT_LEASE_MS = LIFECYCLE_TENANT_EXPIRE_MINUTES * 60_000

export interface LifecycleTenantRunPayload extends BasePayload {
  centralClaim?: {
    claimId: string
  }
}

export class LifecycleTenantRun extends BaseEvent<LifecycleTenantRunPayload> {
  static queueName = 'lifecycle:tenant:run-v1'
  static allowSync = false

  static getQueueOptions(): Queue {
    return { name: this.queueName, policy: 'exactly_once' } as const
  }

  static getSendOptions(payload: LifecycleTenantRunPayload): SendOptions {
    return {
      singletonKey: payload.tenant.ref,
      expireInMinutes: LIFECYCLE_TENANT_EXPIRE_MINUTES,
      retryLimit: 0,
    }
  }

  static getWorkerOptions(): WorkOptions & { concurrentTaskCount: number } {
    return {
      includeMetadata: true,
      concurrentTaskCount: LIFECYCLE_TENANT_CONCURRENCY,
      batchSize: LIFECYCLE_TENANT_CONCURRENCY,
    }
  }

  static async shouldSend(): Promise<boolean> {
    return true
  }

  static async handle(
    job: JobWithMetadata<LifecycleTenantRunPayload>,
    opts?: { signal?: AbortSignal }
  ): Promise<void> {
    const { tenant, centralClaim } = job.data
    const centralStore = centralClaim
      ? new LifecycleTenantStorePg(multitenantPgExecutor)
      : undefined

    // After expiry the scheduler may have re-queued this claim
    // but the store rejects writes past the deadline.
    const deadline = new Date(job.startedOn.getTime() + LIFECYCLE_TENANT_LEASE_MS).toISOString()

    let storage: Awaited<ReturnType<typeof LifecycleTenantRun.createStorage>> | undefined
    try {
      let dispatchState: LifecycleDispatchState | undefined
      let dispatchFloor = 0
      const gateClosed = !(await isLifecycleEnabledForTenant(tenant.ref))
      storage = await this.createStorage(job.data)
      const database = storage.db.asSuperUser()
      if (await database.hasMigration('lifecycle-writer-coordination')) {
        if (!gateClosed) {
          const bucketIds = await database.listDueLifecycleBuckets(LIFECYCLE_TENANT_FANOUT)
          const sent = await Promise.all(
            bucketIds.map((bucketId) => LifecycleBucketRun.send({ tenant, bucketId }))
          )
          if (
            sent.some((jobId) => jobId === null) &&
            !(await EventQueue.getInstance().getQueue(LifecycleBucketRun.getQueueName()))
          ) {
            throw new Error(`Lifecycle queue ${LifecycleBucketRun.getQueueName()} does not exist`)
          }
          if (bucketIds.length > 0 && bucketIds.length < LIFECYCLE_TENANT_FANOUT) {
            dispatchFloor = Date.now() + getConfig().storageLifecycleJobBudgetMs
          }
        }
        await retryAbandonedLifecycleArtifacts({
          storage,
          database,
          attemptedBefore: new Date(
            Date.now() - LIFECYCLE_ABANDONED_RETRY_INTERVAL_MS
          ).toISOString(),
          budgetMs: LIFECYCLE_ABANDONED_RETRY_BUDGET_MS,
          signal: opts?.signal,
        })
        if (centralClaim) {
          dispatchState = await database.findLifecycleDispatchState({
            runBudgetMs: getConfig().storageLifecycleJobBudgetMs,
          })
        }
      }

      if (!centralStore || !centralClaim) return

      const staleFeatureCache =
        gateClosed && (await centralStore.hasObjectVersioningFeature(tenant.ref))
      const now = Date.now()
      let nextDispatchAt = dispatchState?.nextDispatchAt?.getTime() ?? Number.POSITIVE_INFINITY
      if (gateClosed) {
        // A bucket worker that saw the gate open can still create outbox rows after this check.
        // Keep the tenant enrolled so those rows are swept even if the worker crashes.
        nextDispatchAt =
          now +
          (staleFeatureCache
            ? LIFECYCLE_FEATURE_CACHE_RETRY_MS
            : LIFECYCLE_ABANDONED_RETRY_INTERVAL_MS)
      } else {
        nextDispatchAt = Math.max(nextDispatchAt, dispatchFloor)
        if (dispatchState?.hasAbandonedArtifacts) {
          nextDispatchAt = Math.min(nextDispatchAt, now + LIFECYCLE_ABANDONED_RETRY_INTERVAL_MS)
        }
      }
      const completed = await centralStore.completeTenantDispatch(
        tenant.ref,
        centralClaim.claimId,
        new Date(
          Math.min(
            nextDispatchAt,
            now + LIFECYCLE_EVALUATION_INTERVAL_MS + evaluationJitterMs(tenant.ref)
          )
        ).toISOString(),
        deadline,
        dispatchState
      )
      if (!completed) {
        logSchema.warning(logger, '[Lifecycle] Tenant dispatch lost its claim', {
          type: 'event',
          project: tenant.ref,
        })
      }
    } catch (error) {
      if (!centralStore || !centralClaim) throw error

      logSchema.error(logger, '[Lifecycle] Tenant dispatcher failed', {
        type: 'event',
        project: tenant.ref,
        error,
      })
      await centralStore.failTenantDispatch(
        tenant.ref,
        centralClaim.claimId,
        { code: 'tenant_dispatch_failed', message: lifecycleErrorMessage(error) },
        deadline
      )
    } finally {
      storage?.db.destroyConnection()
    }
  }
}
