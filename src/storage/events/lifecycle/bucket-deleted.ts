import { BaseEvent } from '../base-event'
import { BasePayload } from '@internal/queue'
import { BucketType } from '@storage/limits'
import { Job } from 'pg-boss'
import { KnexMetastore } from '@storage/protocols/iceberg/knex'
import { multitenantKnex } from '@internal/database'
import { getConfig } from '../../../config'
import { DeleteIcebergResources } from '@storage/events/iceberg/delete-iceberg-resources'
import { ErrorCode, StorageBackendError } from '@internal/errors'

interface BucketDeletedEvent extends BasePayload {
  bucketId: string
  type: BucketType
}

const { isMultitenant } = getConfig()

export class BucketDeleted extends BaseEvent<BucketDeletedEvent> {
  protected static queueName = 'bucket:deleted'

  static eventName() {
    return `Bucket:Deleted`
  }

  static async handle(job: Job<BucketDeletedEvent>) {
    if (job.data.type !== 'ANALYTICS') {
      return
    }

    const bucketId = job.data.bucketId

    const storage = await this.createStorage(job.data)
    const db = isMultitenant ? multitenantKnex : storage.db.connection.pool.acquire()

    const metastore = new KnexMetastore(db, {
      multiTenant: isMultitenant,
      schema: isMultitenant ? 'public' : 'storage',
    })

    await metastore.transaction(async (metastoreTx) => {
      if (isMultitenant) {
        try {
          await metastoreTx.findCatalogById({
            id: bucketId,
            tenantId: job.data.tenant.ref,
            deleted: true,
          })
        } catch (e) {
          if (e instanceof StorageBackendError && e.code === ErrorCode.NoSuchCatalog) {
            await storage.db.deleteAnalyticsBucket(bucketId)
            return
          }
          throw e
        }
      }

      await metastoreTx.dropCatalog({
        bucketId,
        tenantId: job.data.tenant.ref,
        soft: true,
      })

      await DeleteIcebergResources.send(
        {
          tenant: job.data.tenant,
          catalogId: job.data.bucketId,
        },
        {
          tnx: isMultitenant ? metastoreTx.getTnx() : undefined,
        }
      )

      if (isMultitenant) {
        await storage.db.deleteAnalyticsBucket(bucketId, { soft: true })
      }
    })
  }
}
