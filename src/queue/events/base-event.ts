import { Queue } from '../queue'
import { Job, SendOptions, WorkOptions } from 'pg-boss'
import { getServiceKey } from '../../database/tenant'
import { getPostgrestClient } from '../../database'
import { Storage } from '../../storage'
import { Database } from '../../storage/database'
import { createStorageBackend } from '../../storage/backend'
import { getConfig } from '../../config'

export interface BasePayload {
  $version: string
  tenant: {
    ref: string
    host: string
  }
}

export type StaticThis<T> = { new (...args: any): T }

const { enableQueueEvents } = getConfig()

export abstract class BaseEvent<T extends Omit<BasePayload, '$version'>> {
  public static readonly version: string = 'v1'
  protected static queueName = ''

  protected static queue: typeof Queue

  constructor(public readonly payload: T & BasePayload) {}

  static eventName() {
    return this.name
  }

  static setQueue(queue: typeof Queue) {
    this.queue = queue
  }

  static getQueueName() {
    if (!this.queueName) {
      throw new Error(`Queue name not set on ${this.constructor.name}`)
    }

    return this.queueName
  }

  static getQueueOptions(): SendOptions | undefined {
    return undefined
  }

  static getWorkerOptions(): WorkOptions {
    return {}
  }

  static send<T extends BaseEvent<any>>(this: StaticThis<T>, payload: T['payload']) {
    const that = new this(payload)
    return that.send()
  }

  static async sendWebhook<T extends BaseEvent<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>
  ) {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { Webhook } = require('./webhook')

    await Webhook.send({
      event: {
        type: (this as any).eventName(),
        $version: (this as any).version,
        applyTime: Date.now(),
        payload,
      },
      tenant: payload.tenant,
    })
  }

  static handle(job: Job<BaseEvent<any>['payload']> | Job<BaseEvent<any>['payload']>[]) {
    throw new Error('not implemented')
  }

  send() {
    if (!enableQueueEvents) {
      return
    }

    const constructor = this.constructor as typeof BaseEvent

    return Queue.getInstance().send({
      name: constructor.getQueueName(),
      data: {
        ...this.payload,
        $version: constructor.version,
      },
      options: constructor.getQueueOptions(),
    })
  }

  protected async createStorage(payload: BasePayload) {
    const serviceKey = await getServiceKey(payload.tenant.ref)
    const client = await getPostgrestClient(serviceKey, {
      host: payload.tenant.host,
      tenantId: payload.tenant.ref,
    })

    const db = new Database(client, {
      tenantId: payload.tenant.ref,
      host: payload.tenant.host,
      superAdmin: client,
    })

    const storageBackend = createStorageBackend()

    return new Storage(storageBackend, db)
  }
}
