import { JWK_ALGORITHM_FALLBACK } from '@internal/auth/jwks'
import { jwksManager } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import type { SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import { getConfig, UrlSigningJwkType } from '../../../config'
import { storageEvent } from '../base'

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

export class JwksRollUrlSigningKeyHandler extends QueueHandler(JwksRollUrlSigningKey) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
  }

  async handle(ctx: StorageJob<JwksRollUrlSigningKeyPayload>): Promise<void> {
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
