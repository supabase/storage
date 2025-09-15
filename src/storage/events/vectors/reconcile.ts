import { BaseEvent } from '../base-event'
import { getTenantConfig } from '@internal/database'
import { JobWithMetadata, Queue, SendOptions, WorkOptions } from 'pg-boss'
import { BasePayload } from '@internal/queue'
import { logger, logSchema } from '@internal/monitoring'

interface ResetMigrationsPayload extends BasePayload {
  tenantId: string
}

export class VectorReconcile extends BaseEvent<ResetMigrationsPayload> {
  static queueName = 'vector-reconcile'

  static getQueueOptions(): Queue {
    return {
      name: this.queueName,
      policy: 'exactly_once',
    } as const
  }

  static getWorkerOptions(): WorkOptions {
    return {
      includeMetadata: true,
    }
  }

  static getSendOptions(payload: ResetMigrationsPayload): SendOptions {
    return {
      expireInHours: 2,
      singletonKey: payload.tenantId,
      retryLimit: 3,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: JobWithMetadata<ResetMigrationsPayload>) {
    const tenantId = job.data.tenant.ref
    const tenant = await getTenantConfig(tenantId)

    logSchema.info(logger, `[Migrations] resetting migrations for ${tenantId}`, {
      type: 'migrations',
      project: tenantId,
    })
  }
}
