import {
  type LifecycleTenantClaim,
  LifecycleTenantStorePg,
  multitenantPgExecutor,
} from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { Queue } from '@internal/queue'
import { getConfig } from '../../../config'
import { lifecycleErrorMessage } from '../../lifecycle/executor'
import { LIFECYCLE_TENANT_LEASE_MS, LifecycleTenantRun } from './tenant-run'

const config = getConfig()
const pollIntervalMs = 60_000
const idlePollIntervalMs = 600_000
const pollJitterMs = 5_000
const dispatchLimit = 100
const claimTimeoutMs = 10_000
const claimIdleTimeoutMs = 30_000

// The signal shutdown group must drain before the queue's group.
export function startLifecycleScheduler(signal: AbortSignal): void {
  if (!config.pgQueueEnable || !config.pgQueueEnableWorkers) {
    if (config.storageVersioningEnabled && !config.pgQueueEnable) {
      logSchema.warning(
        logger,
        '[Lifecycle] Lifecycle is enabled but PG_QUEUE_ENABLE is false; no rules will be applied',
        { type: 'event' }
      )
    }
    return
  }
  if (config.storageLifecyclePaused) {
    logSchema.warning(
      logger,
      '[Lifecycle] Lifecycle is paused by STORAGE_LIFECYCLE_PAUSED; no rules will be applied',
      { type: 'event' }
    )
    return
  }

  let timer: NodeJS.Timeout | undefined
  let inFlight = Promise.resolve()
  const idle = !config.isMultitenant && !config.storageVersioningEnabled
  const intervalMs = idle ? idlePollIntervalMs : pollIntervalMs

  const poll = () => {
    inFlight = dispatchDueLifecycleTenants(signal)
      .catch((error) => {
        if (!signal.aborted) {
          logSchema.error(logger, '[Lifecycle] Scheduler poll failed', { type: 'event', error })
        }
      })
      .finally(() => {
        if (signal.aborted) return
        const delay = intervalMs + (Math.random() * 2 - 1) * pollJitterMs
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
  if (!(await Queue.getInstance().getQueue(LifecycleTenantRun.getQueueName()))) {
    throw new Error(`Lifecycle queue ${LifecycleTenantRun.getQueueName()} does not exist`)
  }
  if (!config.isMultitenant) {
    await LifecycleTenantRun.send({ tenant: { ref: config.tenantId, host: '' } })
    return
  }

  const claimSignal = AbortSignal.any([signal, AbortSignal.timeout(claimTimeoutMs)])
  const transaction = await multitenantPgExecutor.beginTransaction({ timeout: claimTimeoutMs })
  let claims: LifecycleTenantClaim[]
  try {
    await transaction.query(
      `SET LOCAL idle_in_transaction_session_timeout = '${claimIdleTimeoutMs}ms'`,
      {
        signal: claimSignal,
      }
    )
    // Leases start at claim time and cap outstanding claims across all schedulers.
    claims = await new LifecycleTenantStorePg(transaction).claimDueTenantsWithinLeaseLimit(
      dispatchLimit,
      LIFECYCLE_TENANT_LEASE_MS,
      claimSignal
    )
    if (signal.aborted) return
    await transaction.commit()
  } finally {
    await transaction.rollback()
  }
  const store = new LifecycleTenantStorePg(multitenantPgExecutor)

  const settle = async (claim: LifecycleTenantClaim, write: Promise<boolean>) => {
    try {
      await write
    } catch (error) {
      logSchema.error(logger, '[Lifecycle] Failed to update tenant dispatch claim', {
        type: 'event',
        project: claim.tenantId,
        error,
      })
    }
  }

  // Finish every claimed dispatch even if persisting one enqueue failure fails.
  await Promise.all(
    claims.map(async (claim) => {
      try {
        const jobId = await LifecycleTenantRun.send({
          tenant: { ref: claim.tenantId, host: '' },
          centralClaim: { claimId: claim.claimId },
        })
        if (jobId === null) {
          // A null send may be a duplicate,
          // leave a reclaimed claim to its job and release a fresh one.
          if (!claim.reclaimed) {
            await settle(
              claim,
              store.releaseTenantDispatch(
                claim.tenantId,
                claim.claimId,
                new Date(Date.now() + pollIntervalMs).toISOString(),
                claim.claimUntil.toISOString()
              )
            )
          }
        } else if (claim.reclaimed) {
          logSchema.warning(logger, '[Lifecycle] Tenant dispatch re-enqueued after claim expiry', {
            type: 'event',
            project: claim.tenantId,
          })
          await settle(claim, store.recordExpiredTenantDispatch(claim.tenantId, claim.claimId))
        }
      } catch (error) {
        logSchema.error(logger, '[Lifecycle] Failed to enqueue tenant dispatcher', {
          type: 'event',
          project: claim.tenantId,
          error,
        })
        if (!claim.reclaimed) {
          await settle(
            claim,
            store.failTenantDispatch(
              claim.tenantId,
              claim.claimId,
              {
                code: 'tenant_dispatch_enqueue_failed',
                message: lifecycleErrorMessage(error),
              },
              claim.claimUntil.toISOString()
            )
          )
        }
      }
    })
  )
}
