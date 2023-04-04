import { Queue } from '../queue'
import { Job, SendOptions, WorkOptions } from 'pg-boss'
import { getServiceKey } from '../../database/tenant'
import { getPostgresConnection } from '../../database'
import { Storage } from '../../storage'
import { StorageKnexDB } from '../../storage/database'
import { createStorageBackend } from '../../storage/backend'
import { getConfig } from '../../config'
import { QueueJobScheduled, QueueJobSchedulingTime } from '../../monitoring/metrics'

export interface BasePayload {
  $version: string
  tenant: {
    ref: string
    host: string
  }
}

export type StaticThis<T> = { new (...args: any): T }

const { enableQueueEvents, isMultitenant, serviceKey } = getConfig()

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

  static getQueueOptions(): SendOptions | undefined {
    return undefined
  }

  static getWorkerOptions(): WorkOptions {
    return {}
  }

  static send<T extends BaseEvent<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>
  ) {
    if (!payload.$version) {
      ;(payload as any).$version = (this as any).version
    }
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

  static handle(job: Job<BaseEvent<any>['payload']>) {
    throw new Error('not implemented')
  }

  protected static async createStorage(payload: BasePayload) {
    const masterServiceKey = isMultitenant ? await getServiceKey(payload.tenant.ref) : serviceKey
    const client = await getPostgresConnection(masterServiceKey, {
      host: payload.tenant.host,
      tenantId: payload.tenant.ref,
    })

    const db = new StorageKnexDB(client, {
      tenantId: payload.tenant.ref,
      host: payload.tenant.host,
      superAdmin: client,
    })

    const storageBackend = createStorageBackend()

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
