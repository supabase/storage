import { multitenantPgExecutor } from '@internal/database'
import { ErrorCode, StorageBackendError } from '@internal/errors'
import type { BasePayload, WirePayload } from '@internal/queue'
import { txQueueCtx } from '@internal/queue'
import { BucketType } from '@storage/limits'
import { PgMetastore } from '@storage/protocols/iceberg/pg'
import type { Storage } from '@storage/storage'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { createStorage, storageEvent } from '../base'
import { DeleteIcebergResources } from '../iceberg/delete-iceberg-resources'
import { getStorageQueue } from '../queue'
import { defaultRetry, TOPICS } from '../topics'

const { isMultitenant, pgQueueConcurrentTasksPerQueue } = getConfig()

export interface BucketDeletedPayload extends BasePayload {
  bucketId: string
  type: BucketType
}

export class BucketDeleted extends storageEvent<BucketDeletedPayload>({
  type: 'Bucket:Deleted',
}) {}

export class BucketDeletedHandler extends TopicHandler(BucketDeleted) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    retry: defaultRetry(TOPICS.bucketDeleted),
  }

  async handle(ctx: JobContext<WirePayload<BucketDeletedPayload>>): Promise<void> {
    const { data } = ctx.message
    if (data.type !== 'ANALYTICS') {
      return
    }

    const bucketId = data.bucketId

    let storage: Storage | undefined

    try {
      storage = await createStorage(data)
      const eventStorage = storage

      const metastore = new PgMetastore(
        isMultitenant ? multitenantPgExecutor : eventStorage.db.connection,
        {
          multiTenant: isMultitenant,
          schema: isMultitenant ? 'public' : 'storage',
        }
      )

      await metastore.transaction(async (metastoreTx) => {
        if (isMultitenant) {
          try {
            await metastoreTx.findCatalogById({
              id: bucketId,
              tenantId: data.tenant.ref,
              deleted: true,
            })
          } catch (e) {
            if (e instanceof StorageBackendError && e.code === ErrorCode.NoSuchCatalog) {
              await eventStorage.db.deleteAnalyticsBucket(bucketId)
              return
            }
            throw e
          }
        }

        await metastoreTx.dropCatalog({
          bucketId,
          tenantId: data.tenant.ref,
          soft: true,
        })

        // Transactional enqueue: the delete-resources job commits with the metastore tx.
        await getStorageQueue().produce(
          new DeleteIcebergResources({
            tenant: data.tenant,
            catalogId: data.bucketId,
            sbReqId: data.sbReqId,
          }),
          isMultitenant ? { ctx: txQueueCtx(metastoreTx.getTnx()) } : undefined
        )

        if (isMultitenant) {
          await eventStorage.db.deleteAnalyticsBucket(bucketId, { soft: true })
        }
      })
    } finally {
      if (storage) {
        storage.db.destroyConnection()
      }
    }
  }
}
