import { BaseEvent } from './base-event'
import { Job, SendOptions, WorkOptions } from 'pg-boss'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload } from '@internal/queue'
import { getDefaultJWKSManager } from '@internal/auth/jwks'

const jwksManager = getDefaultJWKSManager()

interface JwksCreateSigningSecretPayload extends BasePayload {
  tenantId: string
}

export class JwksCreateSigningSecret extends BaseEvent<JwksCreateSigningSecretPayload> {
  static queueName = 'tenants-jwks-create'

  static getWorkerOptions(): WorkOptions {
    return {
      teamSize: 200,
      teamConcurrency: 10,
      includeMetadata: true,
    }
  }

  static getQueueOptions(payload: JwksCreateSigningSecretPayload): SendOptions {
    return {
      expireInHours: 2,
      singletonKey: payload.tenantId,
      useSingletonQueue: true,
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
