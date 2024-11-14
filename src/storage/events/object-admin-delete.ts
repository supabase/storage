import { BaseEvent } from './base-event'
import { getConfig } from '../../config'
import { Job, SendOptions, WorkOptions } from 'pg-boss'
import { withOptionalVersion } from '../disks'
import { logger, logSchema } from '@internal/monitoring'
import { Storage } from '../index'
import { BasePayload } from '@internal/queue'

export interface ObjectDeleteEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
}

const { adminDeleteQueueTeamSize, adminDeleteConcurrency } = getConfig()

export class ObjectAdminDelete extends BaseEvent<ObjectDeleteEvent> {
  static queueName = 'object:admin:delete'

  static getWorkerOptions(): WorkOptions {
    return {
      teamSize: adminDeleteQueueTeamSize,
      teamConcurrency: adminDeleteConcurrency,
    }
  }

  static getQueueOptions(): SendOptions {
    return {
      priority: 10,
    }
  }

  static async handle(job: Job<ObjectDeleteEvent>) {
    let storage: Storage | undefined = undefined

    const version = job.data.version
    const keyToDelete = withOptionalVersion(job.data.name, version)

    try {
      storage = await this.createStorage(job.data)

      logSchema.event(logger, `[Admin]: ObjectAdminDelete ${keyToDelete}`, {
        jodId: job.id,
        type: 'event',
        event: 'ObjectAdminDelete',
        payload: JSON.stringify(job.data),
        objectPath: keyToDelete,
        resources: [`${job.data.bucketId}/${job.data.name}`],
        tenantId: job.data.tenant.ref,
        project: job.data.tenant.ref,
        reqId: job.data.reqId,
      })

      await storage.disk.deleteMany({
        bucket: job.data.bucketId,
        keys: [keyToDelete, keyToDelete + '.info'],
      })
    } catch (e) {
      logger.error(
        {
          error: e,
          jodId: job.id,
          type: 'event',
          event: 'ObjectAdminDelete',
          payload: JSON.stringify(job.data),
          objectPath: keyToDelete,
          objectVersion: job.data.version,
          tenantId: job.data.tenant.ref,
          project: job.data.tenant.ref,
          reqId: job.data.reqId,
        },
        `[Admin]: ObjectAdminDelete ${keyToDelete} - FAILED`
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
              `[Admin]: ObjectAdminDelete ${tenant.ref} - FAILED DISPOSING CONNECTION`
            )
          })
      }
    }
  }
}
