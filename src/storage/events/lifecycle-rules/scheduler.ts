import {
  type LifecycleTenantClaim,
  LifecycleTenantStorePg,
  multitenantPgExecutor,
} from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { PartialSendError } from '@internal/queue/manager'
import { getConfig } from '../../../config'
import { LIFECYCLE_TENANT_LEASE_MS, LifecycleTenantRun } from './tenant-run'

const config = getConfig()
const pollIntervalMs = 60_000
const idlePollIntervalMs = 600_000
const pollJitterMs = 5_000
const claimTimeoutMs = 10_000
const claimIdleTimeoutMs = 30_000
const releaseTimeoutMs = 5_000

// The signal shutdown group must drain before the queue group.
export function startLifecycleScheduler(signal: AbortSignal): void {
  if (!config.pgQueueEnable || !config.pgQueueEnableWorkers) {
    if (config.storageVersioningEnabled && !config.pgQueueEnable) {
      logSchema.warning(
        logger,
        '[LifecycleRule] Lifecycle is enabled but PG_QUEUE_ENABLE is false; no rules will be applied',
        { type: 'lifecycle-rule' }
      )
    }
    return
  }
  if (config.storageLifecyclePaused) {
    logSchema.warning(
      logger,
      '[LifecycleRule] Lifecycle is paused by STORAGE_LIFECYCLE_PAUSED; no rules will be applied',
      { type: 'lifecycle-rule' }
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
          logSchema.error(logger, '[LifecycleRule] Scheduler poll failed', {
            type: 'lifecycle-rule',
            error,
          })
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
  if (!config.isMultitenant) {
    await LifecycleTenantRun.send({ tenant: { ref: config.tenantId, host: '' } }, { signal })
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
      config.storageLifecycleTenantDispatchLimit,
      LIFECYCLE_TENANT_LEASE_MS,
      claimSignal
    )
    if (signal.aborted) return
    await transaction.commit()
  } finally {
    await transaction.rollback()
  }
  if (claims.length === 0) return
  const resent = claims.filter((claim) => claim.reclaimed).length
  if (resent > 0) {
    logSchema.warning(
      logger,
      '[LifecycleRule] Re-sending tenant dispatches after their lease lapsed',
      {
        type: 'lifecycle-rule',
        metadata: JSON.stringify({ resent }),
      }
    )
  }
  try {
    await LifecycleTenantRun.batchSend(
      claims.map((claim) => ({
        tenant: { ref: claim.tenantId, host: '' },
        centralClaim: {
          claimId: claim.claimId,
          claimUntil: claim.claimUntil.toISOString(),
          reclaimed: claim.reclaimed,
        },
      })),
      { signal }
    )
  } catch (error) {
    const unsent = error instanceof PartialSendError ? error.unsent : new Set(claims.keys())
    if (!signal.aborted) {
      logSchema.error(logger, '[LifecycleRule] Failed to enqueue tenant dispatchers', {
        type: 'lifecycle-rule',
        error,
        metadata: JSON.stringify({ unsent: unsent.size }),
      })
    }
    // An enqueue failure or shutdown is not a tenant failure: release the claims for redispatch.
    const store = new LifecycleTenantStorePg(multitenantPgExecutor)
    const releasable = claims.filter((claim, position) => unsent.has(position) && !claim.reclaimed)
    if (releasable.length > 0) {
      try {
        await store.releaseTenantDispatches(releasable, AbortSignal.timeout(releaseTimeoutMs))
      } catch (releaseError) {
        logSchema.error(logger, '[LifecycleRule] Failed to update tenant dispatch claim', {
          type: 'lifecycle-rule',
          error: releaseError,
          metadata: JSON.stringify({ unreleased: releasable.length }),
        })
      }
    }
  }
}
