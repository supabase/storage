import { jwksManager } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, WirePayload } from '@internal/queue'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'
import { systemRetry, TOPICS } from '../topics'

const { pgQueueConcurrentTasksPerQueue } = getConfig()

export interface JwksCreateSigningSecretPayload extends BasePayload {
  tenantId: string
}

export class JwksCreateSigningSecret extends storageEvent<JwksCreateSigningSecretPayload>({
  type: 'JwksCreateSigningSecret',
  idempotencyKey: (data) => data.tenantId,
}) {}

export class JwksCreateSigningSecretHandler extends TopicHandler(JwksCreateSigningSecret) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
    retry: systemRetry(TOPICS.jwksCreateSigningSecret),
  }

  async handle(ctx: JobContext<WirePayload<JwksCreateSigningSecretPayload>>): Promise<void> {
    const { tenantId, sbReqId } = ctx.message.data

    try {
      const { kid } = await jwksManager.generateUrlSigningJwk(tenantId)

      logSchema.info(
        logger,
        `[Jwks] create new url signing secret (${kid}) for tenant ${tenantId}`,
        {
          type: 'jwks',
          project: tenantId,
          sbReqId,
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
          sbReqId,
        }
      )
      throw e
    }
  }
}
