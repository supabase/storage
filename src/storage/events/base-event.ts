import { Event as QueueBaseEvent, BasePayload, StaticThis, Event } from '@internal/queue'
import { getPostgresConnection, getServiceKeyUser } from '@internal/database'
import { StorageKnexDB } from '../database'
import { createStorageBackend, StorageBackendAdapter } from '../backend'
import { Storage } from '../storage'
import { getConfig } from '../../config'
import { logger } from '@internal/monitoring'
import { createAgent } from '@internal/http'
import { TenantLocation } from '@storage/locator'

const { storageS3Bucket, storageS3MaxSockets, storageBackendType, region } = getConfig()

let storageBackend: StorageBackendAdapter | undefined = undefined

export abstract class BaseEvent<T extends Omit<BasePayload, '$version'>> extends QueueBaseEvent<T> {
  static onStart() {
    this.getOrCreateStorageBackend()
  }

  static onClose() {
    storageBackend?.close()
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
    const { Webhook } = require('./lifecycle/webhook')
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

    return new Storage(this.getOrCreateStorageBackend(), db, new TenantLocation(storageS3Bucket))
  }

  protected static getOrCreateStorageBackend() {
    if (storageBackend) {
      return storageBackend
    }

    const httpAgents = {
      api: createAgent('s3_worker_api', {
        maxSockets: storageS3MaxSockets,
      }),
      download: createAgent('s3_worker_download', {
        maxSockets: storageS3MaxSockets,
      }),
      upload: createAgent('s3_worker_upload', {
        maxSockets: storageS3MaxSockets,
      }),
    }

    storageBackend = createStorageBackend(storageBackendType, {
      httpAgents,
    })

    return storageBackend
  }
}
