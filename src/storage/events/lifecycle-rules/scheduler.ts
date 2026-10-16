import { LifecycleTenantStorePg, multitenantPgExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { getConfig } from '../../../config'
import { isLifecycleExecutionEnabled } from '../../lifecycle/guards'
import { LifecycleTenantRun } from './tenant-run'
import { LIFECYCLE_DISPATCH_LIMIT, lifecycleErrorMessage } from './utils'

const config = getConfig()
const pollIntervalMs = 60_000
const pollJitterMs = 5_000
const maxDispatchBatches = 10

/** The signal's shutdown group must drain before the queue's group. */
export function startLifecycleScheduler(signal: AbortSignal): void {
  if (
    signal.aborted ||
    !isLifecycleExecutionEnabled() ||
    !config.pgQueueEnable ||
    !config.pgQueueEnableWorkers
  ) {
    return
  }

  let timer: NodeJS.Timeout | undefined
  let inFlight = Promise.resolve()

  const poll = () => {
    inFlight = dispatchDueLifecycleTenants(signal)
      .catch((error) => {
        logSchema.error(logger, '[Lifecycle] Scheduler poll failed', { type: 'event', error })
      })
      .finally(() => {
        if (signal.aborted) return
        const delay = pollIntervalMs + (Math.random() * 2 - 1) * pollJitterMs
        timer = setTimeout(poll, delay)
        timer.unref()
      })
  }

  signal.addEventListener(
    'abort',
    () => {
      clearTimeout(timer)
      return inFlight
    },
    { once: true }
  )
  poll()
}

export async function dispatchDueLifecycleTenants(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return
  if (!config.isMultitenant) {
    await LifecycleTenantRun.send({ tenant: { ref: config.tenantId, host: '' } })
    return
  }

  const store = new LifecycleTenantStorePg(multitenantPgExecutor)
  for (let batch = 0; batch < maxDispatchBatches && !signal.aborted; batch++) {
    const claims = await store.claimDueTenants(
      LIFECYCLE_DISPATCH_LIMIT,
      config.storageLifecycleClaimLeaseMs
    )

    // Finish every claimed dispatch, even if persisting one enqueue failure fails.
    const results = await Promise.allSettled(
      claims.map(async (claim) => {
        try {
          await LifecycleTenantRun.send({
            tenant: { ref: claim.tenantId, host: '' },
            centralClaim: { claimId: claim.claimId },
          })
        } catch (error) {
          logSchema.error(logger, '[Lifecycle] Failed to enqueue tenant dispatcher', {
            type: 'event',
            project: claim.tenantId,
            error,
          })
          await store.failTenantDispatch(claim.tenantId, claim.claimId, {
            code: 'tenant_dispatch_enqueue_failed',
            message: lifecycleErrorMessage(error),
          })
          return
        }
      })
    )
    for (const [index, result] of results.entries()) {
      if (result.status === 'rejected') {
        logSchema.error(logger, '[Lifecycle] Failed to release tenant dispatch claim', {
          type: 'event',
          project: claims[index].tenantId,
          error: result.reason,
        })
      }
    }
    if (claims.length < LIFECYCLE_DISPATCH_LIMIT) return
  }
}
