import {
  deleteTenantConfig,
  LifecycleTenantStorePg,
  multitenantPgExecutor,
} from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import { QueueHandler, type SubscribeOptions } from '@internal/queue/manager'
import { getConfig } from '../../../config'
import { retryAbandonedLifecycleArtifacts } from '../../lifecycle/abandoned-artifacts'
import {
  evaluationJitterMs,
  LIFECYCLE_EVALUATION_INTERVAL_MS,
  lifecycleErrorMessage,
} from '../../lifecycle/executor'
import { isLifecycleEnabledForTenant } from '../../lifecycle/guards'
import type { LifecycleDispatchState } from '../../schemas'
import { createStorage, storageEvent } from '../base'
import { dispatchLifecycleBuckets, LIFECYCLE_RELEASE_TIMEOUT_MS } from './bucket-run'

const LIFECYCLE_TENANT_CONCURRENCY = 4
const LIFECYCLE_TENANT_EXPIRE_MINUTES = 3
const LIFECYCLE_FEATURE_CACHE_RETRY_MS = 5 * 60_000
const LIFECYCLE_ABANDONED_RETRY_INTERVAL_MS = 60 * 60_000
const LIFECYCLE_ABANDONED_RETRY_BUDGET_MS = 60_000
const LIFECYCLE_TENANT_REVISIT_FLOOR_CAP_MS = 60_000
const LIFECYCLE_CLOSED_GATE_FAILURE_BACKOFF_MAX_SECONDS = 24 * 60 * 60
const LIFECYCLE_TIMEOUT_BACKOFF_MAX_SECONDS = 60 * 60
export const LIFECYCLE_TENANT_LEASE_MS = LIFECYCLE_TENANT_EXPIRE_MINUTES * 60_000

export interface LifecycleTenantRunPayload extends BasePayload {
  centralClaim?: {
    claimId: string
    claimUntil: string
    reclaimed?: boolean
  }
}

export class LifecycleTenantRun extends storageEvent<LifecycleTenantRunPayload>({
  type: 'LifecycleTenantRun',
  allowSync: false,
  disableKeys: () => [],
  idempotencyKey: (data) =>
    data.centralClaim
      ? `${data.tenant.ref}/${data.centralClaim.claimId}/${data.centralClaim.claimUntil}`
      : data.tenant.ref,
}) {}

export class LifecycleTenantRunHandler extends QueueHandler(LifecycleTenantRun) {
  override readonly options: SubscribeOptions = {
    prefetch: LIFECYCLE_TENANT_CONCURRENCY,
    parallelism: LIFECYCLE_TENANT_CONCURRENCY,
    // The abort fires before the claim lapses so the run can still settle its claim.
    consumeTimeout: LIFECYCLE_TENANT_LEASE_MS - 30_000,
  }

  async handle(ctx: StorageJob<LifecycleTenantRunPayload>): Promise<void> {
    const { tenant, centralClaim } = ctx.message.data
    const centralStore = centralClaim
      ? new LifecycleTenantStorePg(multitenantPgExecutor)
      : undefined
    if (ctx.signal.aborted) throw ctx.signal.reason
    const started =
      centralStore && centralClaim
        ? await centralStore.startTenantDispatch(
            tenant.ref,
            centralClaim.claimId,
            centralClaim.claimUntil,
            LIFECYCLE_TENANT_LEASE_MS,
            centralClaim.reclaimed === true
          )
        : undefined
    if (centralClaim && !started) return
    const claimId = started?.claimId
    const deadline = started?.claimUntil.toISOString()

    let storage: Awaited<ReturnType<typeof createStorage>> | undefined
    let gateClosed = false
    try {
      let dispatchState: LifecycleDispatchState | undefined
      gateClosed = !(await isLifecycleEnabledForTenant(tenant.ref))
      if (ctx.signal.aborted) throw ctx.signal.reason
      storage = await createStorage(ctx.message.data)
      const database = storage.db.asSuperUser()
      if (await database.hasMigration('lifecycle-writer-coordination')) {
        if (!gateClosed) {
          if (ctx.signal.aborted) throw ctx.signal.reason
          await dispatchLifecycleBuckets(tenant, database, ctx.signal)
        }
        await retryAbandonedLifecycleArtifacts({
          storage,
          database,
          attemptedBefore: new Date(
            Date.now() - LIFECYCLE_ABANDONED_RETRY_INTERVAL_MS
          ).toISOString(),
          budgetMs: LIFECYCLE_ABANDONED_RETRY_BUDGET_MS,
          signal: ctx.signal,
        })
        if (centralClaim) {
          if (ctx.signal.aborted) throw ctx.signal.reason
          dispatchState = await database.findLifecycleDispatchState({
            runBudgetMs: getConfig().storageLifecycleJobBudgetMs,
          })
        }
      }

      if (!centralStore || !claimId || !deadline) return

      const staleFeatureCache =
        gateClosed && (await centralStore.hasObjectVersioningFeature(tenant.ref, ctx.signal))
      if (staleFeatureCache) deleteTenantConfig(tenant.ref)
      // An abort can end the sweep without an error: settle it as an abort, not a success.
      if (ctx.signal.aborted) throw ctx.signal.reason
      const now = Date.now()
      let nextDispatchAt = dispatchState?.nextDispatchAt?.getTime() ?? Number.POSITIVE_INFINITY
      if (gateClosed) {
        // A bucket worker that saw the gate open can still create outbox rows after this check.
        // Back off quietly from 1h to the daily interval so late rows are still swept without
        // polling idle gate-closed tenants hourly forever.
        nextDispatchAt = staleFeatureCache
          ? now + LIFECYCLE_FEATURE_CACHE_RETRY_MS
          : dispatchState?.hasAbandonedArtifacts
            ? now + LIFECYCLE_ABANDONED_RETRY_INTERVAL_MS
            : now +
              Math.max(
                LIFECYCLE_ABANDONED_RETRY_INTERVAL_MS,
                dispatchState?.quietMs ?? Number.POSITIVE_INFINITY
              )
      } else {
        // Avoid spin-looping when more than FANOUT due buckets wait behind queued markers.
        if (nextDispatchAt <= now) {
          nextDispatchAt =
            now +
            Math.min(getConfig().storageLifecycleJobBudgetMs, LIFECYCLE_TENANT_REVISIT_FLOOR_CAP_MS)
        }
        if (dispatchState?.hasAbandonedArtifacts) {
          nextDispatchAt = Math.min(nextDispatchAt, now + LIFECYCLE_ABANDONED_RETRY_INTERVAL_MS)
        }
      }
      const nextDispatch = new Date(
        Math.min(
          nextDispatchAt,
          now + LIFECYCLE_EVALUATION_INTERVAL_MS + evaluationJitterMs(tenant.ref)
        )
      ).toISOString()
      let completed: boolean
      try {
        completed = await centralStore.completeTenantDispatch(
          tenant.ref,
          claimId,
          nextDispatch,
          deadline,
          dispatchState
        )
      } catch (completeError) {
        logSchema.error(logger, '[LifecycleRule] Tenant dispatch completion failed', {
          type: 'lifecycle-rule',
          project: tenant.ref,
          error: completeError,
        })
        await releaseClaim(centralStore, tenant.ref, claimId, deadline)
        return
      }
      if (!completed) {
        logSchema.warning(logger, '[LifecycleRule] Tenant dispatch lost its claim', {
          type: 'lifecycle-rule',
          project: tenant.ref,
        })
      }
    } catch (error) {
      if (!centralStore || !claimId || !deadline) throw error

      if (ctx.signal.aborted) {
        if (isShutdown(ctx.signal)) {
          // Shutdown is not a tenant failure: release the claim for immediate redispatch.
          await releaseClaim(centralStore, tenant.ref, claimId, deadline)
        } else if (isTimeout(ctx.signal)) {
          ;(gateClosed ? logSchema.warning : logSchema.error)(
            logger,
            '[LifecycleRule] Tenant dispatcher timed out',
            {
              type: 'lifecycle-rule',
              project: tenant.ref,
              error,
            }
          )
          await centralStore
            .failTenantDispatch(
              tenant.ref,
              claimId,
              {
                code: 'tenant_dispatch_timeout',
                message: 'Tenant dispatch exceeded consume timeout',
              },
              deadline,
              gateClosed
                ? LIFECYCLE_CLOSED_GATE_FAILURE_BACKOFF_MAX_SECONDS
                : LIFECYCLE_TIMEOUT_BACKOFF_MAX_SECONDS,
              AbortSignal.timeout(LIFECYCLE_RELEASE_TIMEOUT_MS)
            )
            .catch((failError) =>
              logSchema.warning(
                logger,
                '[LifecycleRule] Tenant dispatch timeout settlement failed',
                {
                  type: 'lifecycle-rule',
                  project: tenant.ref,
                  error: failError,
                }
              )
            )
        }
        // Unknown abort reasons leave the claim to lapse and be reclaimed.
        throw error
      }

      ;(gateClosed ? logSchema.warning : logSchema.error)(
        logger,
        '[LifecycleRule] Tenant dispatcher failed',
        {
          type: 'lifecycle-rule',
          project: tenant.ref,
          error,
        }
      )
      await centralStore.failTenantDispatch(
        tenant.ref,
        claimId,
        { code: 'tenant_dispatch_failed', message: lifecycleErrorMessage(error) },
        deadline,
        gateClosed ? LIFECYCLE_CLOSED_GATE_FAILURE_BACKOFF_MAX_SECONDS : undefined
      )
    } finally {
      storage?.db.destroyConnection()
    }
  }
}

function releaseClaim(
  store: LifecycleTenantStorePg,
  tenantRef: string,
  claimId: string,
  deadline: string
): Promise<unknown> {
  return store
    .releaseTenantDispatch(
      tenantRef,
      claimId,
      deadline,
      AbortSignal.timeout(LIFECYCLE_RELEASE_TIMEOUT_MS)
    )
    .catch((releaseError) =>
      logSchema.warning(logger, '[LifecycleRule] Tenant dispatch release failed', {
        type: 'lifecycle-rule',
        project: tenantRef,
        error: releaseError,
      })
    )
}

function isShutdown(signal: AbortSignal): boolean {
  return (
    signal.aborted && signal.reason instanceof DOMException && signal.reason.name === 'AbortError'
  )
}

function isTimeout(signal: AbortSignal): boolean {
  return (
    signal.aborted && signal.reason instanceof DOMException && signal.reason.name === 'TimeoutError'
  )
}
