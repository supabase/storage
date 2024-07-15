import { Event as QueueBaseEvent, BasePayload, StaticThis, Event } from '@internal/queue'
import { getPostgresConnection, getServiceKeyUser } from '@internal/database'
import { logger } from '@internal/monitoring'

import { StorageKnexDB } from '../database'
import { createAgent, createDisk, createDefaultDisk } from '../disks'
import { Storage } from '../storage'
import { getConfig } from '../../config'

const { webhookMaxConnections, region, storageBackendType } = getConfig()

const httpAgent = createAgent({
  maxSockets: webhookMaxConnections,
})

export abstract class BaseEvent<T extends Omit<BasePayload, '$version'>> extends QueueBaseEvent<T> {
  static getBucketId(payload: any): string | undefined {
    return
  }

  /**
   * Sends a message as a webhook
   * @param payload
   */
  static async sendWebhook<T extends Event<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>
  ) {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { Webhook } = require('./webhook')
    const eventType = this.eventName()

    try {
      await Webhook.send({
        event: {
          type: eventType,
          region,
          $version: this.version,
          applyTime: Date.now(),
          payload,
        },
        tenant: payload.tenant,
      })
    } catch (e) {
      logger.error(
        {
          error: e,
          event: {
            type: eventType,
            $version: this.version,
            applyTime: Date.now(),
            payload: JSON.stringify(payload),
          },
          tenant: payload.tenant,
        },
        `error sending webhook: ${eventType}`
      )
    }
  }

  protected static async createStorage(payload: BasePayload) {
    const adminUser = await getServiceKeyUser(payload.tenant.ref)

    const client = await getPostgresConnection({
      user: adminUser,
      superUser: adminUser,
      host: payload.tenant.host,
      tenantId: payload.tenant.ref,
      disableHostCheck: true,
    })

    const db = new StorageKnexDB(client, {
      tenantId: payload.tenant.ref,
      host: payload.tenant.host,
    })

    const bucketId = this.getBucketId(payload)

    if (bucketId) {
      const bucket = await db.findBucketById(bucketId, 'buckets.id', {
        withDisk: true,
      })

      const credentials = bucket.credentials

      if (credentials) {
        return new Storage(
          createDisk(storageBackendType, {
            httpAgent,
            bucket: bucket.mount_point,
            accessKey: credentials.access_key,
            secretKey: credentials.secret_key,
            endpoint: credentials.endpoint,
            forcePathStyle: credentials.force_path_style,
            region: credentials.region,
          }),
          db
        )
      }
    }

    const storageBackend = createDefaultDisk({
      httpAgent,
      prefix: payload.tenant.ref,
    })

    return new Storage(storageBackend, db)
  }
}
