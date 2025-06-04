import { BaseEvent } from '../base-event'
import { Job, Queue, SendOptions, WorkOptions } from 'pg-boss'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload } from '@internal/queue'
import { jwksManager } from '@internal/database'

interface JwksCreateSigningSecretPayload extends BasePayload {
  tenantId: string
}

export class JwksCreateSigningSecret extends BaseEvent<JwksCreateSigningSecretPayload> {
  static queueName = 'tenants-jwks-create'

  static getQueueOptions(): Queue {
    return {
      name: this.queueName,
      policy: 'stately',
    } as const
  }

  static getWorkerOptions(): WorkOptions {
    return {
      includeMetadata: true,
    }
  }

  static getSendOptions(payload: JwksCreateSigningSecretPayload): SendOptions {
    return {
      expireInHours: 2,
      singletonKey: payload.tenantId,
      retryLimit: 3,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: Job<JwksCreateSigningSecretPayload>) {
    const { tenantId } = job.data

    try {
      const { kid } = await jwksManager.generateUrlSigningJwk(tenantId)

      logSchema.info(
        logger,
        `[Jwks] create new url signing secret (${kid}) for tenant ${tenantId}`,
        {
          type: 'jwks',
          project: tenantId,
        }
      )
    } catch (e) {
      logSchema.error(
        logger,
        `[Jwks] create new url signing secret failed for tenant ${tenantId}`,
        {
          type: 'jwks',
          error: e,
          project: tenantId,
        }
      )
      throw e
    }
  }
}
