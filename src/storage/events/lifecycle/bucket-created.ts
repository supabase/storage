import { BaseEvent } from '../base-event'
import { BasePayload } from '@internal/queue'
import { BucketType } from '@storage/limits'
import { Job } from 'pg-boss'
import { getCatalogAuthStrategy, TenantAwareRestCatalog } from '@storage/protocols/iceberg/catalog'
import { KnexMetastore } from '@storage/protocols/iceberg/knex'
import { getTenantConfig, multitenantKnex } from '@internal/database'
import { getConfig } from '../../../config'

interface ObjectCreatedEvent extends BasePayload {
  bucketId: string
  type: BucketType
}

const { icebergCatalogAuthType, icebergWarehouse, icebergCatalogUrl, isMultitenant } = getConfig()

const catalogAuthType = getCatalogAuthStrategy(icebergCatalogAuthType)

export class BucketCreatedEvent extends BaseEvent<ObjectCreatedEvent> {
  protected static queueName = 'bucket:created'

  static eventName() {
    return `Bucket:Created`
  }

  static async handle(job: Job<ObjectCreatedEvent>) {
    if (!isMultitenant || job.data.type !== 'ANALYTICS') {
      return
    }

    const { features } = await getTenantConfig(job.data.tenant.ref)

    const restCatalog = new TenantAwareRestCatalog({
      tenantId: job.data.tenant.ref,
      limits: {
        maxNamespaceCount: features.icebergCatalog.maxNamespaces,
        maxTableCount: features.icebergCatalog.maxTables,
        maxCatalogsCount: features.icebergCatalog.maxCatalogs,
      },
      restCatalogUrl: icebergCatalogUrl,
      warehouse: icebergWarehouse,
      auth: catalogAuthType,
      metastore: new KnexMetastore(multitenantKnex, {
        multiTenant: true,
        schema: 'public',
      }),
    })

    await restCatalog.registerCatalog({
      bucketId: job.data.bucketId,
      tenantId: job.data.tenant.ref,
    })
  }
}
