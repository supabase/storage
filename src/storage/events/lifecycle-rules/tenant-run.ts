import {
  deleteTenantConfig,
  LifecycleTenantStorePg,
  multitenantPgExecutor,
} from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import {
  isShutdownSignal,
  isTimeoutSignal,
  QueueHandler,
  type SubscribeOptions,
} from '@internal/queue/manager'
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
const LIFECYCLE_IDLE_REVISIT_INTERVAL_MS = 30 * LIFECYCLE_EVALUATION_INTERVAL_MS
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
    if (ctx.signal.aborted) throw ctx.signal.reason
    const claim = centralClaim
      ? await startTenantDispatchClaim(tenant.ref, centralClaim)
      : undefined
    if (centralClaim && !claim) return

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
        if (claim) {
          if (ctx.signal.aborted) throw ctx.signal.reason
          dispatchState = await database.findLifecycleDispatchState({
            runBudgetMs: getConfig().storageLifecycleJobBudgetMs,
          })
        }
      }

      if (!claim) return

      const staleFeatureCache = gateClosed && (await claim.hasObjectVersioningFeature(ctx.signal))
      if (staleFeatureCache) deleteTenantConfig(tenant.ref)
      // An abort can end the sweep without an error: settle it as an abort, not a success.
      if (ctx.signal.aborted) throw ctx.signal.reason
      const nextDispatchAt = computeNextTenantDispatchAt({
        now: Date.now(),
        tenantRef: tenant.ref,
        dispatchState,
        gateClosed,
        staleFeatureCache,
        jobBudgetMs: getConfig().storageLifecycleJobBudgetMs,
      })
      await claim.complete(nextDispatchAt, dispatchState)
    } catch (error) {
      if (!claim) throw error

      if (ctx.signal.aborted) {
        if (isShutdownSignal(ctx.signal)) {
          // Shutdown is not a tenant failure: release the claim for immediate redispatch.
          await claim.release()
        } else if (isTimeoutSignal(ctx.signal)) {
          ;(gateClosed ? logSchema.warning : logSchema.error)(
            logger,
            '[LifecycleRule] Tenant dispatcher timed out',
            {
              type: 'lifecycle-rule',
              project: tenant.ref,
              error,
            }
          )
          await claim
            .fail(
              {
                code: 'tenant_dispatch_timeout',
                message: 'Tenant dispatch exceeded consume timeout',
              },
              {
                maxBackoffSeconds: gateClosed
                  ? LIFECYCLE_CLOSED_GATE_FAILURE_BACKOFF_MAX_SECONDS
                  : LIFECYCLE_TIMEOUT_BACKOFF_MAX_SECONDS,
                signal: AbortSignal.timeout(LIFECYCLE_RELEASE_TIMEOUT_MS),
              }
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
      await claim.fail(
        { code: 'tenant_dispatch_failed', message: lifecycleErrorMessage(error) },
        {
          maxBackoffSeconds: gateClosed
            ? LIFECYCLE_CLOSED_GATE_FAILURE_BACKOFF_MAX_SECONDS
            : undefined,
        }
      )
    } finally {
      storage?.db.destroyConnection()
    }
  }
}

interface TenantDispatchClaim {
  hasObjectVersioningFeature(signal: AbortSignal): Promise<boolean>
  complete(nextDispatchAt: Date, dispatchState: LifecycleDispatchState | undefined): Promise<void>
  fail(
    error: Record<string, unknown>,
    options?: { maxBackoffSeconds?: number; signal?: AbortSignal }
  ): Promise<boolean>
  release(): Promise<unknown>
}

async function startTenantDispatchClaim(
  tenantRef: string,
  centralClaim: NonNullable<LifecycleTenantRunPayload['centralClaim']>
): Promise<TenantDispatchClaim | undefined> {
  const store = new LifecycleTenantStorePg(multitenantPgExecutor)
  const started = await store.startTenantDispatch(
    tenantRef,
    centralClaim.claimId,
    centralClaim.claimUntil,
    LIFECYCLE_TENANT_LEASE_MS,
    centralClaim.reclaimed === true
  )
  if (!started) return undefined

  const claimId = started.claimId
  const deadline = started.claimUntil.toISOString()
  const startedAt = started.startedAt.toISOString()

  const release = () =>
    store
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

  return {
    hasObjectVersioningFeature(signal) {
      return store.hasObjectVersioningFeature(tenantRef, signal)
    },
    async complete(nextDispatchAt, dispatchState) {
      const nextDispatch = nextDispatchAt.toISOString()
      let completed: boolean
      try {
        completed = await store.completeTenantDispatch(
          tenantRef,
          claimId,
          nextDispatch,
          deadline,
          dispatchState,
          startedAt
        )
      } catch (completeError) {
        logSchema.error(logger, '[LifecycleRule] Tenant dispatch completion failed', {
          type: 'lifecycle-rule',
          project: tenantRef,
          error: completeError,
        })
        await release()
        return
      }
      if (!completed) {
        logSchema.warning(logger, '[LifecycleRule] Tenant dispatch lost its claim', {
          type: 'lifecycle-rule',
          project: tenantRef,
        })
      }
    },
    fail(error, options) {
      return options?.signal !== undefined
        ? store.failTenantDispatch(
            tenantRef,
            claimId,
            error,
            deadline,
            options.maxBackoffSeconds,
            options.signal
          )
        : store.failTenantDispatch(tenantRef, claimId, error, deadline, options?.maxBackoffSeconds)
    },
    release,
  }
}

export function computeNextTenantDispatchAt(params: {
  now: number
  tenantRef: string
  dispatchState: LifecycleDispatchState | undefined
  gateClosed: boolean
  staleFeatureCache: boolean
  jobBudgetMs: number
}): Date {
  const { now, tenantRef, dispatchState, gateClosed, staleFeatureCache, jobBudgetMs } = params
  let nextDispatchAt = dispatchState?.nextDispatchAt?.getTime() ?? Number.POSITIVE_INFINITY
  if (gateClosed) {
    // A bucket worker that saw the gate open can still create outbox rows after this check.
    // Back off quietly from 1h to the idle revisit cap so late rows are still swept without
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
      nextDispatchAt = now + Math.min(jobBudgetMs, LIFECYCLE_TENANT_REVISIT_FLOOR_CAP_MS)
    }
    if (dispatchState?.hasAbandonedArtifacts) {
      nextDispatchAt = Math.min(nextDispatchAt, now + LIFECYCLE_ABANDONED_RETRY_INTERVAL_MS)
    }
  }
  const isIdle =
    dispatchState !== undefined &&
    !dispatchState.hasAbandonedArtifacts &&
    (gateClosed || dispatchState.nextDispatchAt === null)
  const capAt = isIdle
    ? nextIdleRevisitAt(now, tenantRef)
    : now + LIFECYCLE_EVALUATION_INTERVAL_MS + evaluationJitterMs(tenantRef)
  return new Date(Math.min(nextDispatchAt, capAt))
}

export function nextIdleRevisitAt(
  nowMs: number,
  tenantRef: string,
  periodMs = LIFECYCLE_IDLE_REVISIT_INTERVAL_MS
): number {
  const phaseMs = evaluationJitterMs(tenantRef, periodMs)
  const remaining = (((phaseMs - (nowMs % periodMs)) % periodMs) + periodMs) % periodMs
  return nowMs + (remaining < LIFECYCLE_EVALUATION_INTERVAL_MS ? remaining + periodMs : remaining)
}
