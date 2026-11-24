import { logger, logSchema } from '@internal/monitoring'
import { lifecycleRunOutcomes } from '@internal/monitoring/metrics'
import type { BasePayload, StorageJob } from '@internal/queue'
import { PartialSendError, QueueHandler, type SubscribeOptions } from '@internal/queue/manager'
import { getConfig } from '../../../config'
import type { Database } from '../../database'
import { NoncurrentLifecycleExecutor } from '../../lifecycle/executor'
import { isLifecycleEnabledForTenant } from '../../lifecycle/guards'
import { createStorage, storageEvent } from '../base'

const config = getConfig()

const LIFECYCLE_BUCKET_CONCURRENCY = 8
const LIFECYCLE_TENANT_FANOUT = 8
const LIFECYCLE_BUCKET_REDISPATCH_MS = 15 * 60_000
export const LIFECYCLE_RELEASE_TIMEOUT_MS = 5_000
export const LIFECYCLE_BUCKET_MAX_DELIVERY_MS = 24 * 60 * 60_000 - 60_000
const LIFECYCLE_BUCKET_DELIVERY_HEADROOM_MS = 60_000
export const LIFECYCLE_BUCKET_DELIVERY_MS = Math.min(
  LIFECYCLE_BUCKET_MAX_DELIVERY_MS,
  Math.ceil(
    (config.storageLifecycleJobBudgetMs +
      config.storageLifecycleClaimLeaseMs +
      LIFECYCLE_BUCKET_DELIVERY_HEADROOM_MS) /
      60_000
  ) * 60_000
)

export interface LifecycleBucketRunPayload extends BasePayload {
  bucketId: string
  dispatchId: string
}

export class LifecycleBucketRun extends storageEvent<LifecycleBucketRunPayload>({
  type: 'LifecycleBucketRun',
  allowSync: false,
  disableKeys: () => [],
  idempotencyKey: (data) => `${data.tenant.ref}/${data.bucketId}/${data.dispatchId}`,
}) {}

export async function dispatchLifecycleBuckets(
  tenant: BasePayload['tenant'],
  database: Database,
  signal: AbortSignal,
  maxDispatches?: number
): Promise<void> {
  const dispatches = await database.dispatchDueLifecycleBuckets({
    limit: LIFECYCLE_TENANT_FANOUT,
    redispatchMs: LIFECYCLE_BUCKET_REDISPATCH_MS,
    leaseMs: config.storageLifecycleClaimLeaseMs,
    maxDispatches,
  })
  if (dispatches.length === 0) return
  try {
    await LifecycleBucketRun.batchSend(
      dispatches.map(({ bucketId, dispatchId }) => ({ tenant, bucketId, dispatchId })),
      { signal }
    )
  } catch (sendError) {
    const unsent =
      sendError instanceof PartialSendError
        ? dispatches.filter((_, position) => sendError.unsent.has(position))
        : dispatches
    await database
      .releaseDispatchedLifecycleBuckets(unsent, AbortSignal.timeout(LIFECYCLE_RELEASE_TIMEOUT_MS))
      .catch((releaseError) =>
        logSchema.warning(logger, '[LifecycleRule] Failed to release dispatched buckets', {
          type: 'lifecycle-rule',
          project: tenant.ref,
          error: releaseError,
        })
      )
    throw sendError
  }
}

export class LifecycleBucketRunHandler extends QueueHandler(LifecycleBucketRun) {
  override readonly options: SubscribeOptions = {
    prefetch: LIFECYCLE_BUCKET_CONCURRENCY,
    parallelism: LIFECYCLE_BUCKET_CONCURRENCY,
    consumeTimeout: LIFECYCLE_BUCKET_DELIVERY_MS,
    heartbeatIntervalMs: 30_000,
  }

  async handle(ctx: StorageJob<LifecycleBucketRunPayload>): Promise<void> {
    let status = 'ERROR'
    let storage: Awaited<ReturnType<typeof createStorage>> | undefined
    try {
      storage = await createStorage(ctx.message.data)
      const result = await new NoncurrentLifecycleExecutor(storage).run(ctx.message.data.bucketId, {
        tenantLifecycleEnabled: await isLifecycleEnabledForTenant(ctx.message.data.tenant.ref),
        signal: ctx.signal,
      })
      status = result.status
      logSchema.info(logger, '[LifecycleRule] Bucket delivery completed', {
        type: 'lifecycle-rule',
        project: ctx.message.data.tenant.ref,
        metadata: JSON.stringify({
          bucketId: ctx.message.data.bucketId,
          runId: result.runId,
          status: result.status,
        }),
      })
      // The run freed one of the tenant slots: dispatch the next due bucket without waiting
      // for the tenant next visit. At most one, so runs ending together cannot overshoot the limit.
      // Only after a clean finish or budget stop so a failing backend is not walked through every due bucket.
      if ((result.status === 'COMPLETED' || result.reason === 'budget') && !ctx.signal.aborted) {
        await dispatchLifecycleBuckets(
          ctx.message.data.tenant,
          storage.db.asSuperUser(),
          ctx.signal,
          1
        ).catch((error) =>
          logSchema.warning(
            logger,
            '[LifecycleRule] Failed to dispatch the next lifecycle bucket',
            {
              type: 'lifecycle-rule',
              project: ctx.message.data.tenant.ref,
              error,
            }
          )
        )
      }
    } finally {
      lifecycleRunOutcomes.add(1, { status })
      storage?.db.destroyConnection()
    }
  }
}
