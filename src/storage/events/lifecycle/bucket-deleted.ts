import { BaseEvent } from '../base-event'
import { BasePayload } from '@internal/queue'
import { BucketType } from '@storage/limits'
import { Job } from 'pg-boss'
import { KnexMetastore } from '@storage/protocols/iceberg/knex'
import { multitenantKnex } from '@internal/database'
import { getConfig } from '../../../config'
import { ERRORS } from '@internal/errors'

interface BucketDeletedEvent extends BasePayload {
  bucketId: string
  type: BucketType
}

const { isMultitenant } = getConfig()

export class BucketDeleted extends BaseEvent<BucketDeletedEvent> {
  protected static queueName = 'bucket:created'

  static eventName() {
    return `Bucket:Deleted`
  }

  static async handle(job: Job<BucketDeletedEvent>) {
    if (job.data.type !== 'ANALYTICS') {
      return
    }

    const db = isMultitenant
      ? multitenantKnex
      : (await this.createStorage(job.data)).db.connection.pool.acquire()

    const metastore = new KnexMetastore(db, {
      multiTenant: isMultitenant,
      schema: isMultitenant ? 'public' : 'storage',
    })

    const resources = await metastore.countResources({
      tenantId: job.data.tenant.ref,
      bucketId: job.data.bucketId,
      limit: 1000,
    })

    if (resources.namespaces > 0 || resources.tables > 0) {
      throw ERRORS.BucketNotEmpty(job.data.bucketId)
    }
  }
}
