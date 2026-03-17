import { getPostgresConnection, getServiceKeyUser } from '@internal/database'
import { createAgent } from '@internal/http'
import { logger } from '@internal/monitoring'
import { BasePayload, Event, EventPayload, Event as QueueBaseEvent } from '@internal/queue'
import { TenantLocation } from '@storage/locator'
import { getConfig } from '../../config'
import { createStorageBackend, StorageBackendAdapter } from '../backend'
import { StorageKnexDB } from '../database'
import { Storage } from '../storage'

const { storageS3Bucket, storageS3MaxSockets, storageBackendType, region } = getConfig()

let storageBackend: StorageBackendAdapter | undefined = undefined

export abstract class BaseEvent<T extends EventPayload> extends QueueBaseEvent<T> {
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
  static async sendWebhook<TThis extends abstract new (...args: never[]) => Event<EventPayload>>(
    this: TThis & { version: string; eventName(): string },
    payload: Omit<InstanceType<TThis>['payload'], '$version'>
  ) {
    // biome-ignore lint/style/noCommonJs: build script runs as CommonJS
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

  protected static getOrCreateStorageBackend(monitor = false) {
    if (storageBackend) {
      return storageBackend
    }

    const httpAgent = createAgent('s3_worker', {
      maxSockets: storageS3MaxSockets,
    })

    storageBackend = createStorageBackend(storageBackendType, {
      httpAgent,
    })

    if (monitor) {
      httpAgent.monitor()
    }

    return storageBackend
  }
}
