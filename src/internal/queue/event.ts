import { Queue } from './queue'
import PgBoss, { Job, SendOptions, WorkOptions, Queue as PgBossQueue } from 'pg-boss'
import { getConfig } from '../../config'
import { QueueJobScheduled, QueueJobSchedulingTime } from '@internal/monitoring/metrics'
import { logger, logSchema } from '@internal/monitoring'
import { getTenantConfig } from '@internal/database'
import { ERRORS } from '@internal/errors'

export interface BasePayload {
  $version?: string
  singletonKey?: string
  scheduleAt?: Date
  reqId?: string
  tenant: {
    ref: string
    host: string
  }
}

const { pgQueueEnable, region, isMultitenant } = getConfig()

export type StaticThis<T extends Event<any>> = BaseEventConstructor<T>

interface BaseEventConstructor<Base extends Event<any>> {
  version: string

  new (...args: any): Base

  send(
    this: StaticThis<Base>,
    payload: Omit<Base['payload'], '$version'>
  ): Promise<string | void | null>

  eventName(): string
  getWorkerOptions(): WorkOptions
}

/**
 * Base class for all events that are sent to the queue
 */
export class Event<T extends Omit<BasePayload, '$version'>> {
  public static readonly version: string = 'v1'
  protected static queueName = ''
  protected static allowSync = true

  constructor(public readonly payload: T & BasePayload) {}

  static eventName() {
    return this.name
  }

  static deadLetterQueueName() {
    return this.queueName + '-dead-letter'
  }

  static getQueueName() {
    if (!this.queueName) {
      throw new Error(`Queue name not set on ${this.constructor.name}`)
    }

    return this.queueName
  }

  static getQueueOptions(): PgBossQueue | undefined {
    return undefined
  }

  static getSendOptions<T extends Event<any>>(payload: T['payload']): SendOptions | undefined {
    return undefined
  }

  static getWorkerOptions(): WorkOptions & { concurrentTaskCount?: number } {
    return {}
  }

  static onClose() {
    // no-op
  }

  static onStart() {
    // no-op
  }

  static batchSend<T extends Event<any>[]>(messages: T) {
    if (!pgQueueEnable) {
      if (this.allowSync) {
        return Promise.all(messages.map((message) => message.send()))
      } else {
        logger.warn(
          {
            type: 'queue',
            eventType: this.eventName(),
          },
          '[Queue] skipped sending batch messages'
        )
        return
      }
    }

    return Queue.getInstance().insert(
      messages.map((message) => {
        const sendOptions = (this.getSendOptions(message.payload) as PgBoss.JobInsert) || {}
        if (!message.payload.$version) {
          ;(message.payload as (typeof message)['payload']).$version = this.version
        }

        if (message.payload.scheduleAt) {
          sendOptions.startAfter = new Date(message.payload.scheduleAt)
        }

        return {
          ...sendOptions,
          name: this.getQueueName(),
          data: message.payload,
          deadLetter: this.deadLetterQueueName(),
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

  static invoke<T extends Event<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>
  ) {
    if (!payload.$version) {
      ;(payload as T['payload']).$version = this.version
    }
    const that = new this(payload)
    return that.invoke()
  }

  static invokeOrSend<T extends Event<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>,
    options?: SendOptions
  ) {
    if (!payload.$version) {
      ;(payload as T['payload']).$version = this.version
    }
    const that = new this(payload)
    return that.invokeOrSend(options)
  }

  static handle(
    job: Job<Event<any>['payload']> | Job<Event<any>['payload']>[],
    opts?: { signal?: AbortSignal }
  ) {
    throw new Error('not implemented')
  }

  static async shouldSend(payload: any) {
    if (isMultitenant && payload?.tenant?.ref) {
      // Do not send an event if disabled for this specific tenant
      const tenant = await getTenantConfig(payload.tenant.ref)
      const disabledEvents = tenant.disableEvents || []
      if (disabledEvents.includes(this.eventName())) {
        return false
      }
    }
    return true
  }

  /**
   * See issue https://github.com/timgit/pg-boss/issues/535
   * @param queueName
   * @param singletonKey
   * @param jobId
   */
  static async deleteIfActiveExists(queueName: string, singletonKey: string, jobId: string) {
    if (!pgQueueEnable) {
      return Promise.resolve()
    }

    await Queue.getDb().executeSql(
      `DELETE FROM pgboss_v10.job
       WHERE id = $1
       AND EXISTS(
          SELECT 1 FROM pgboss_v10.job
             WHERE id != $2
             AND state < 'active'
             AND name = $3
             AND singleton_key = $4
       )
      `,
      [jobId, jobId, queueName, singletonKey]
    )
  }

  async invokeOrSend(sendOptions?: SendOptions): Promise<string | void | null> {
    const constructor = this.constructor as typeof Event

    if (!constructor.allowSync) {
      throw ERRORS.InternalError(undefined, 'Cannot send this event synchronously')
    }

    try {
      await this.invoke()
    } catch (e) {
      logSchema.error(logger, '[Queue] Error invoking event synchronously, sending to queue', {
        type: 'queue',
        project: this.payload.tenant?.ref,
        error: e,
        metadata: JSON.stringify(this.payload),
      })

      return this.send(sendOptions)
    }
  }

  async invoke(): Promise<string | void | null> {
    const constructor = this.constructor as typeof Event

    if (!constructor.allowSync) {
      throw ERRORS.InternalError(undefined, 'Cannot send this event synchronously')
    }

    await constructor.handle({
      id: '__sync',
      expireInSeconds: 0,
      name: constructor.getQueueName(),
      data: {
        region,
        ...this.payload,
        $version: constructor.version,
      },
    })
  }

  async send(customSendOptions?: SendOptions): Promise<string | void | null> {
    const constructor = this.constructor as typeof Event

    const shouldSend = await constructor.shouldSend(this.payload)

    if (!shouldSend) {
      return
    }

    if (!pgQueueEnable) {
      if (constructor.allowSync) {
        return constructor.handle({
          id: '__sync',
          expireInSeconds: 0,
          name: constructor.getQueueName(),
          data: {
            region,
            ...this.payload,
            $version: constructor.version,
          },
        })
      } else {
        logger.warn(
          {
            type: 'queue',
            eventType: constructor.eventName(),
          },
          '[Queue] skipped sending message'
        )
        return
      }
    }

    const timer = QueueJobSchedulingTime.startTimer()
    const sendOptions = constructor.getSendOptions(this.payload) || {}

    if (this.payload.scheduleAt) {
      sendOptions.startAfter = new Date(this.payload.scheduleAt)
    }

    sendOptions!.deadLetter = constructor.deadLetterQueueName()

    try {
      const res = await Queue.getInstance().send({
        name: constructor.getQueueName(),
        data: {
          region,
          ...this.payload,
          $version: constructor.version,
        },
        options: {
          ...sendOptions,
          ...customSendOptions,
        },
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
        expireInSeconds: 0,
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
}
