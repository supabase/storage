import { JWK_ALGORITHM_FALLBACK } from '@internal/auth/jwks'
import { jwksManager } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import type { SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import { getConfig, UrlSigningJwkType } from '../../../config'
import { storageEvent } from '../base'

const { pgQueueConcurrentTasksPerQueue } = getConfig()

export interface JwksCreateSigningSecretPayload extends BasePayload {
  tenantId: string
  keyType: UrlSigningJwkType
}

export class JwksCreateSigningSecret extends storageEvent<JwksCreateSigningSecretPayload>({
  type: 'JwksCreateSigningSecret',
  idempotencyKey: (data) => data.tenantId,
  // A fleet-wide fan-out: never gated per tenant, so sending one loads no tenant config.
  disableKeys: () => [],
}) {}

export class JwksCreateSigningSecretHandler extends QueueHandler(JwksCreateSigningSecret) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
  }

  async handle(ctx: StorageJob<JwksCreateSigningSecretPayload>): Promise<void> {
    const { tenantId, sbReqId, keyType } = ctx.message.data

    try {
      const { kid } = await jwksManager.generateUrlSigningJwk(
        tenantId,
        keyType || JWK_ALGORITHM_FALLBACK
      )

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
