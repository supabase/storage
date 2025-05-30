import { BaseEvent } from '../base-event'
import { getConfig } from '../../../config'
import { Job, SendOptions, WorkOptions } from 'pg-boss'
import { logger, logSchema } from '@internal/monitoring'
import { Storage } from '../../index'
import { BasePayload } from '@internal/queue'

export interface ObjectDeleteBatchEvent extends BasePayload {
  prefixes: string[]
  bucketId: string
}

const { storageS3Bucket, adminDeleteQueueTeamSize, adminDeleteConcurrency } = getConfig()

export class ObjectAdminDeleteBatch extends BaseEvent<ObjectDeleteBatchEvent> {
  static queueName = 'object:admin:delete-batch'

  static getWorkerOptions(): WorkOptions {
    return {}
  }

  static getSendOptions(): SendOptions {
    return {
      priority: 10,
      expireInSeconds: 30,
    }
  }

  static async handle(job: Job<ObjectDeleteBatchEvent>) {
    let storage: Storage | undefined = undefined

    const { prefixes, bucketId } = job.data
    if (prefixes.length < 1) {
      return
    }

    try {
      storage = await this.createStorage(job.data)

      logSchema.event(logger, `[Admin]: ObjectAdminDeleteBatch ${bucketId} ${prefixes.length}`, {
        jodId: job.id,
        type: 'event',
        event: 'ObjectAdminDeleteBatch',
        payload: JSON.stringify(job.data),
        objectPath: bucketId,
        resources: prefixes,
        tenantId: job.data.tenant.ref,
        project: job.data.tenant.ref,
        reqId: job.data.reqId,
      })

      await storage.backend.deleteObjects(storageS3Bucket, prefixes)
    } catch (e) {
      logger.error(
        {
          error: e,
          jodId: job.id,
          type: 'event',
          event: 'ObjectAdminDeleteBatch',
          payload: JSON.stringify(job.data),
          objectPath: bucketId,
          resources: prefixes,
          tenantId: job.data.tenant.ref,
          project: job.data.tenant.ref,
          reqId: job.data.reqId,
        },
        `[Admin]: ObjectAdminDeleteBatch ${bucketId} ${prefixes.length} - FAILED`
      )
      throw e
    } finally {
      if (storage) {
        const tenant = storage.db.tenant()
        storage.db
          .destroyConnection()
          .then(() => {
            // no-op
          })
          .catch((e) => {
            logger.error(
              { error: e },
              `[Admin]: ObjectAdminDeleteBatch ${tenant.ref} - FAILED DISPOSING CONNECTION`
            )
          })
      }
    }
  }
}
