import { JWK_ALGORITHM_FALLBACK } from '@internal/auth/jwks'
import { jwksManager } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, WirePayload } from '@internal/queue'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig, UrlSigningJwkType } from '../../../config'
import { storageEvent } from '../base'
import { systemRetry, TOPICS } from '../topics'

const { pgQueueConcurrentTasksPerQueue } = getConfig()

export interface JwksRollUrlSigningKeyPayload extends BasePayload {
  tenantId: string
  keyType: UrlSigningJwkType
}

export class JwksRollUrlSigningKey extends storageEvent<JwksRollUrlSigningKeyPayload>({
  type: 'JwksRollUrlSigningKey',
  idempotencyKey: (data) => `jwks_roll_url_signing_key_${data.tenantId}`,
  // v1 overrode `shouldSend` to always true — this event ignores per-tenant disableEvents.
  disableKeys: () => [],
}) {}

export class JwksRollUrlSigningKeyHandler extends TopicHandler(JwksRollUrlSigningKey) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
    retry: systemRetry(TOPICS.jwksRollUrlSigningKey),
  }

  async handle(ctx: JobContext<WirePayload<JwksRollUrlSigningKeyPayload>>): Promise<void> {
    const { tenantId, sbReqId, keyType } = ctx.message.data

    try {
      const { oldKid, newKid } = await jwksManager.rollUrlSigningJwk(
        tenantId,
        keyType || JWK_ALGORITHM_FALLBACK
      )

      logSchema.info(
        logger,
        `[Jwks] rolled url signing key for tenant ${tenantId} (old: ${oldKid}, new: ${newKid})`,
        {
          type: 'jwks',
          project: tenantId,
          sbReqId,
        }
      )
    } catch (e) {
      logSchema.error(logger, `[Jwks] roll url signing key failed for tenant ${tenantId}`, {
        type: 'jwks',
        error: e,
        project: tenantId,
        sbReqId,
      })
      throw e
    }
  }
}
