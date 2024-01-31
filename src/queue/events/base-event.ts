import { Queue } from '../queue'
import PgBoss, { BatchWorkOptions, Job, SendOptions, WorkOptions } from 'pg-boss'
import { getPostgresConnection, getServiceKeyUser } from '../../database'
import { Storage } from '../../storage'
import { StorageKnexDB } from '../../storage/database'
import { createAgent, createStorageBackend } from '../../storage/backend'
import { getConfig } from '../../config'
import { QueueJobScheduled, QueueJobSchedulingTime } from '../../monitoring/metrics'
import { logger } from '../../monitoring'

export interface BasePayload {
  $version?: string
  singletonKey?: string
  reqId?: string
  tenant: {
    ref: string
    host: string
  }
}

export interface SlowRetryQueueOptions {
  retryLimit: number
  retryDelay: number
}

const { pgQueueEnable, storageBackendType, storageS3Endpoint } = getConfig()
const storageS3Protocol = storageS3Endpoint?.includes('http://') ? 'http' : 'https'
const httpAgent = createAgent(storageS3Protocol)

type StaticThis<T extends BaseEvent<any>> = BaseEventConstructor<T>

interface BaseEventConstructor<Base extends BaseEvent<any>> {
  version: string

  new (...args: any): Base

  send(
    this: StaticThis<Base>,
    payload: Omit<Base['payload'], '$version'>
  ): Promise<string | void | null>

  eventName(): string
  getWorkerOptions(): WorkOptions | BatchWorkOptions
}

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

  static getQueueOptions<T extends BaseEvent<any>>(payload: T['payload']): SendOptions | undefined {
    return undefined
  }

  static getWorkerOptions(): WorkOptions | BatchWorkOptions {
    return {}
  }

  static withSlowRetryQueue(): undefined | SlowRetryQueueOptions {
    return undefined
  }

  static getSlowRetryQueueName() {
    if (!this.queueName) {
      throw new Error(`Queue name not set on ${this.constructor.name}`)
    }

    return this.queueName + '-slow'
  }

  static batchSend<T extends BaseEvent<any>[]>(messages: T) {
    return Queue.getInstance().insert(
      messages.map((message) => {
        const sendOptions = (this.getQueueOptions(message.payload) as PgBoss.JobInsert) || {}
        if (!message.payload.$version) {
          ;(message.payload as (typeof message)['payload']).$version = this.version
        }
        return {
          ...sendOptions,
          name: this.getQueueName(),
          data: message.payload,
        }
      })
    )
  }

  static send<T extends BaseEvent<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>
  ) {
    if (!payload.$version) {
      ;(payload as T['payload']).$version = this.version
    }
    const that = new this(payload)
    return that.send()
  }

  static sendSlowRetryQueue<T extends BaseEvent<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>
  ) {
    if (!payload.$version) {
      ;(payload as T['payload']).$version = this.version
    }
    const that = new this(payload)
    return that.sendSlowRetryQueue()
  }

  static async sendWebhook<T extends BaseEvent<any>>(
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

  static handle(job: Job<BaseEvent<any>['payload']> | Job<BaseEvent<any>['payload']>[]) {
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

    const storageBackend = createStorageBackend(storageBackendType, {
      httpAgent,
    })

    return new Storage(storageBackend, db)
  }

  async send(): Promise<string | void | null> {
    const constructor = this.constructor as typeof BaseEvent

    if (!pgQueueEnable) {
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
    const sendOptions = constructor.getQueueOptions(this.payload)

    const res = await Queue.getInstance().send({
      name: constructor.getQueueName(),
      data: {
        ...this.payload,
        $version: constructor.version,
      },
      options: sendOptions,
    })

    timer({
      name: constructor.getQueueName(),
    })

    QueueJobScheduled.inc({
      name: constructor.getQueueName(),
    })

    return res
  }

  async sendSlowRetryQueue() {
    const constructor = this.constructor as typeof BaseEvent
    const slowRetryQueue = constructor.withSlowRetryQueue()

    if (!pgQueueEnable || !slowRetryQueue) {
      return
    }

    const timer = QueueJobSchedulingTime.startTimer()
    const sendOptions = constructor.getQueueOptions(this.payload) || {}

    const res = await Queue.getInstance().send({
      name: constructor.getSlowRetryQueueName(),
      data: {
        ...this.payload,
        $version: constructor.version,
      },
      options: {
        retryBackoff: true,
        startAfter: 60 * 60 * 30, // 30 mins
        ...sendOptions,
        ...slowRetryQueue,
      },
    })

    timer({
      name: constructor.getSlowRetryQueueName(),
    })

    QueueJobScheduled.inc({
      name: constructor.getSlowRetryQueueName(),
    })

    return res
  }
}
