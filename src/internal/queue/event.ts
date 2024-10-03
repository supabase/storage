import { Queue } from './queue'
import PgBoss, { BatchWorkOptions, Job, SendOptions, WorkOptions } from 'pg-boss'
import { getConfig } from '../../config'
import { QueueJobScheduled, QueueJobSchedulingTime } from '@internal/monitoring/metrics'
import { logger, logSchema } from '@internal/monitoring'

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

const { pgQueueEnable, region } = getConfig()

export type StaticThis<T extends Event<any>> = BaseEventConstructor<T>

interface BaseEventConstructor<Base extends Event<any>> {
  version: string

  new (...args: any): Base

  send(
    this: StaticThis<Base>,
    payload: Omit<Base['payload'], '$version'>
  ): Promise<string | void | null>

  eventName(): string
  getWorkerOptions(): WorkOptions | BatchWorkOptions
}

/**
 * Base class for all events that are sent to the queue
 */
export class Event<T extends Omit<BasePayload, '$version'>> {
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

  static getQueueOptions<T extends Event<any>>(payload: T['payload']): SendOptions | undefined {
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

  static onClose() {
    // no-op
  }

  static onStart() {
    // no-op
  }

  static batchSend<T extends Event<any>[]>(messages: T) {
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

  static send<T extends Event<any>>(this: StaticThis<T>, payload: Omit<T['payload'], '$version'>) {
    if (!payload.$version) {
      ;(payload as T['payload']).$version = this.version
    }
    const that = new this(payload)
    return that.send()
  }

  static sendSlowRetryQueue<T extends Event<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>
  ) {
    if (!payload.$version) {
      ;(payload as T['payload']).$version = this.version
    }
    const that = new this(payload)
    return that.sendSlowRetryQueue()
  }

  static handle(job: Job<Event<any>['payload']> | Job<Event<any>['payload']>[]) {
    throw new Error('not implemented')
  }

  async send(): Promise<string | void | null> {
    const constructor = this.constructor as typeof Event

    if (!pgQueueEnable) {
      return constructor.handle({
        id: '__sync',
        name: constructor.getQueueName(),
        data: {
          region,
          ...this.payload,
          $version: constructor.version,
        },
      })
    }

    const timer = QueueJobSchedulingTime.startTimer()
    const sendOptions = constructor.getQueueOptions(this.payload)

    try {
      const res = await Queue.getInstance().send({
        name: constructor.getQueueName(),
        data: {
          region,
          ...this.payload,
          $version: constructor.version,
        },
        options: sendOptions,
      })

      QueueJobScheduled.inc({
        name: constructor.getQueueName(),
      })

      return res
    } catch (e) {
      // If we can't queue the message for some reason,
      // we run its handler right away.
      // This might create some latency with the benefit of being more fault-tolerant
      logSchema.warning(
        logger,
        `[Queue Sender] Error while sending job to queue, sending synchronously`,
        {
          type: 'queue',
          error: e,
          metadata: JSON.stringify(this.payload),
        }
      )
      return constructor.handle({
        id: '__sync',
        name: constructor.getQueueName(),
        data: {
          region,
          ...this.payload,
          $version: constructor.version,
        },
      })
    } finally {
      timer({
        name: constructor.getQueueName(),
      })
    }
  }

  async sendSlowRetryQueue() {
    const constructor = this.constructor as typeof Event
    const slowRetryQueue = constructor.withSlowRetryQueue()

    if (!pgQueueEnable || !slowRetryQueue) {
      return
    }

    const timer = QueueJobSchedulingTime.startTimer()
    const sendOptions = constructor.getQueueOptions(this.payload) || {}

    const res = await Queue.getInstance().send({
      name: constructor.getSlowRetryQueueName(),
      data: {
        region,
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
