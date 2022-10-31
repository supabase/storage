import { Queue } from '../queue'
import { Job, SendOptions, WorkOptions } from 'pg-boss'
import { getServiceKey } from '../../database/tenant'
import { getPostgrestClient } from '../../database'
import { Storage } from '../../storage'
import { Database } from '../../storage/database'
import { createStorageBackend } from '../../storage/backend'

export interface BasePayload {
  project: {
    ref: string
    host: string
  }
}

export type StaticThis<T> = { new (...args: any): T }

export abstract class BaseEvent<T extends object> {
  constructor(public readonly payload: T & BasePayload) {}

  protected static queueName = ''

  static eventName() {
    return this.name
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

  static async sendWebhook<T extends BaseEvent<any>>(this: StaticThis<T>, payload: T['payload']) {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { Webhook } = require('./webhook')

    console.log((this as any).eventName(), 'eventName')
    await Webhook.send({
      eventName: (this as any).eventName(),
      payload: payload,
      project: payload.project,
      applyTime: Date.now(),
    })
  }

  static handle(job: Job<BaseEvent<any>['payload']>) {
    throw new Error('not implemented')
  }

  send() {
    const constructor = this.constructor as typeof BaseEvent

    return Queue.getInstance().send({
      name: constructor.getQueueName(),
      data: this.payload,
      options: constructor.getQueueOptions(),
    })
  }

  protected async createStorage(payload: BasePayload) {
    const serviceKey = await getServiceKey(payload.project.ref)
    const client = await getPostgrestClient(serviceKey, {
      host: payload.project.host,
      tenantId: payload.project.ref,
    })

    const db = new Database(client, {
      tenantId: payload.project.ref,
      host: payload.project.host,
      superAdmin: client,
    })

    const storageBackend = createStorageBackend()

    return new Storage(storageBackend, db)
  }
}
