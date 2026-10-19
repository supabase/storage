import { LifecycleTenantStorePg, multitenantPgExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload } from '@internal/queue'
import { JobWithMetadata, Queue, SendOptions, WorkOptions } from 'pg-boss'
import { LIFECYCLE_EVALUATION_INTERVAL_MS } from '../../lifecycle/executor'
import { isLifecycleEnabledForTenant } from '../../lifecycle/guards'
import type { LifecycleDispatchState } from '../../schemas'
import { BaseEvent } from '../base-event'
import { LifecycleBucketShardRun } from './bucket-shard-run'
import { LIFECYCLE_DISPATCH_LIMIT, lifecycleErrorMessage } from './utils'

const LIFECYCLE_TENANT_CONCURRENCY = 4

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
      expireInMinutes: 3,
      retryLimit: 0,
    }
  }

  static getWorkerOptions(): WorkOptions & { concurrentTaskCount: number } {
    return {
      includeMetadata: true,
      concurrentTaskCount: LIFECYCLE_TENANT_CONCURRENCY,
      pollingIntervalSeconds: 5,
    }
  }

  static async shouldSend(): Promise<boolean> {
    // Generic webhook/event suppression must not strand a central claim.
    return true
  }

  static async handle(
    job: JobWithMetadata<LifecycleTenantRunPayload>,
    opts?: { signal?: AbortSignal }
  ): Promise<void> {
    if (opts?.signal?.aborted) return

    const { tenant, centralClaim } = job.data
    const centralStore = centralClaim
      ? new LifecycleTenantStorePg(multitenantPgExecutor)
      : undefined

    let storage: Awaited<ReturnType<typeof LifecycleTenantRun.createStorage>> | undefined
    try {
      let dispatchState: LifecycleDispatchState | undefined
      const inFlightOnly = !(await isLifecycleEnabledForTenant(tenant.ref))
      storage = await this.createStorage(job.data)
      const database = storage.db.asSuperUser()
      // Enrollment can precede the tenant migration during rollout or restore.
      if (await database.hasMigration('lifecycle-writer-coordination')) {
        const coordinates = await database.listDueLifecycleShards(LIFECYCLE_DISPATCH_LIMIT, {
          inFlightOnly,
        })
        await Promise.all(
          coordinates.map((coordinate) => LifecycleBucketShardRun.send({ tenant, ...coordinate }))
        )
        dispatchState = await database.findLifecycleDispatchState({ inFlightOnly })
      }

      if (!centralStore || !centralClaim) return

      const completed = await centralStore.completeTenantDispatch(
        tenant.ref,
        centralClaim.claimId,
        capCentralDispatch(dispatchState?.nextDispatchAt ?? null),
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
      await centralStore.failTenantDispatch(tenant.ref, centralClaim.claimId, {
        code: 'tenant_dispatch_failed',
        message: lifecycleErrorMessage(error),
      })
    } finally {
      storage?.db.destroyConnection()
    }
  }
}

function capCentralDispatch(value: Date | null): string {
  const requested = value ? value.getTime() : Number.POSITIVE_INFINITY
  const maximum = Date.now() + LIFECYCLE_EVALUATION_INTERVAL_MS
  return new Date(Math.min(requested, maximum)).toISOString()
}
