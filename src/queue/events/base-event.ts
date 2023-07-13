import { Queue } from '../queue'
import { Job, SendOptions, WorkOptions } from 'pg-boss'
import { getServiceKeyUser } from '../../database/tenant'
import { getPostgresConnection } from '../../database'
import { Storage } from '../../storage'
import { StorageKnexDB } from '../../storage/database'
import { createStorageBackend } from '../../storage/backend'
import { getConfig } from '../../config'
import { QueueJobScheduled, QueueJobSchedulingTime } from '../../monitoring/metrics'
import { logger } from '../../monitoring'

export interface BasePayload {
  $version: string
  tenant: {
    ref: string
    host: string
  }
}

type StaticThis<T extends BaseEvent<any>> = BaseEventConstructor<T>

interface BaseEventConstructor<Base extends BaseEvent<any>> {
  version: string

  new (...args: any): Base

  send(
    this: StaticThis<Base>,
    payload: Omit<Base['payload'], '$version'>
  ): Promise<string | void | null>

  eventName(): string
  beforeSend(payload: Omit<Base['payload'], '$version'>): Promise<Base['payload']>
}

const { enableQueueEvents } = getConfig()

export abstract class BaseEvent<T extends Omit<BasePayload, '$version'>> {
  public static readonly version: string = 'v1'
  protected static queueName = ''

  constructor(public readonly payload: T & BasePayload) {}

  static eventName() {
    return this.name
  }

  static getQueueName() {
    if (!this.queueName) {
      throw new Error(`Queue name not set on ${this.constructor.name}`)
    }

    return this.queueName
  }

  static async beforeSend<T extends BaseEvent<any>>(payload: Omit<T['payload'], '$version'>) {
    return payload
  }

  static getQueueOptions(): SendOptions | undefined {
    return undefined
  }

  static getWorkerOptions(): WorkOptions {
    return {}
  }

  static async send<T extends BaseEvent<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>
  ) {
    if (!payload.$version) {
      ;(payload as T['payload']).$version = this.version
    }
    const newPayload = await this.beforeSend(payload)

    const that = new this(newPayload)
    return that.send()
  }

  static async sendWebhook<T extends BaseEvent<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>
  ) {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { Webhook } = require('./webhook')
    const eventType = this.eventName()
    const newPayload = await this.beforeSend(payload)

    try {
      await Webhook.send({
        event: {
          type: eventType,
          $version: this.version,
          applyTime: Date.now(),
          payload: newPayload,
        },
        tenant: newPayload.tenant,
      })
    } catch (e) {
      logger.error(
        {
          event: {
            type: eventType,
            $version: this.version,
            applyTime: Date.now(),
            payload: newPayload,
          },
          tenant: newPayload.tenant,
        },
        `error sending webhook: ${eventType}`
      )
    }
  }

  static handle(job: Job<BaseEvent<any>['payload']>) {
    throw new Error('not implemented')
  }

  protected static async createStorage(payload: BasePayload) {
    const adminUser = await getServiceKeyUser(payload.tenant.ref)

    const client = await getPostgresConnection({
      user: adminUser,
      superUser: adminUser,
      host: payload.tenant.host,
      tenantId: payload.tenant.ref,
    })

    const db = new StorageKnexDB(client, {
      tenantId: payload.tenant.ref,
      host: payload.tenant.host,
    })

    const storageBackend = await createStorageBackend(payload.tenant.ref)

    return new Storage(storageBackend, db)
  }

  async send() {
    const constructor = this.constructor as typeof BaseEvent

    if (!enableQueueEvents) {
      return constructor.handle({
        id: '',
        name: constructor.getQueueName(),
        data: {
          ...this.payload,
          $version: constructor.version,
        },
      })
    }

    const timer = QueueJobSchedulingTime.startTimer()

    const res = await Queue.getInstance().send({
      name: constructor.getQueueName(),
      data: {
        ...this.payload,
        $version: constructor.version,
      },
      options: constructor.getQueueOptions(),
    })

    timer({
      name: constructor.getQueueName(),
      tenant_id: this.payload.tenant.ref,
    })

    QueueJobScheduled.inc({
      name: constructor.getQueueName(),
      tenant_id: this.payload.tenant.ref,
    })

    return res
  }
}
