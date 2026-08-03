import { getTenantConfig, multitenantPgExecutor } from '@internal/database'
import type { BasePayload, WirePayload } from '@internal/queue'
import { PgShardStoreFactory, ShardCatalog, SingleShard } from '@internal/sharding'
import { BucketType } from '@storage/limits'
import { getCatalogAuthStrategy, TenantAwareRestCatalog } from '@storage/protocols/iceberg/catalog'
import { PgMetastore } from '@storage/protocols/iceberg/pg'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'
import { defaultRetry, TOPICS } from '../topics'

export interface BucketCreatedPayload extends BasePayload {
  bucketId: string
  bucketName: string
  type: BucketType
}

const {
  icebergCatalogAuthType,
  icebergWarehouse,
  icebergCatalogUrl,
  isMultitenant,
  pgQueueConcurrentTasksPerQueue,
} = getConfig()

const catalogAuthType = getCatalogAuthStrategy(icebergCatalogAuthType)

export class BucketCreatedEvent extends storageEvent<BucketCreatedPayload>({
  type: 'Bucket:Created',
}) {}

/**
 * Registered as a real worker (unlike v1, which never polled `bucket:created`, so its
 * invoke-failure fallback enqueued jobs nobody could deliver — a silent drop, now fixed).
 */
export class BucketCreatedHandler extends TopicHandler(BucketCreatedEvent) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    retry: defaultRetry(TOPICS.bucketCreated),
  }

  async handle(ctx: JobContext<WirePayload<BucketCreatedPayload>>): Promise<void> {
    const { data } = ctx.message
    if (!isMultitenant || data.type !== 'ANALYTICS') {
      return
    }

    const { features } = await getTenantConfig(data.tenant.ref)

    const restCatalog = new TenantAwareRestCatalog({
      tenantId: data.tenant.ref,
      limits: {
        maxNamespaceCount: features.icebergCatalog.maxNamespaces,
        maxTableCount: features.icebergCatalog.maxTables,
        maxCatalogsCount: features.icebergCatalog.maxCatalogs,
      },
      restCatalogUrl: icebergCatalogUrl,
      sharding: isMultitenant
        ? new ShardCatalog(new PgShardStoreFactory(multitenantPgExecutor))
        : new SingleShard({
            shardKey: icebergWarehouse,
            capacity: 10000,
          }),
      auth: catalogAuthType,
      metastore: new PgMetastore(multitenantPgExecutor, {
        multiTenant: true,
        schema: 'public',
      }),
    })

    await restCatalog.registerCatalog({
      bucketId: data.bucketId,
      bucketName: data.bucketName,
      tenantId: data.tenant.ref,
    })
  }
}
