import { getTenantConfig } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import { queueJobScheduled, queueJobSchedulingTime } from '@internal/monitoring/metrics'
import { KnexQueueDB } from '@internal/queue/database'
import { Knex } from 'knex'
import PgBoss, { Job, Queue as PgBossQueue, SendOptions, WorkOptions } from 'pg-boss'
import { getConfig } from '../../config'
import { PG_BOSS_SCHEMA, Queue } from './queue'

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

function withPayloadVersion<TPayload extends BasePayload>(
  payload: TPayload,
  version: string
): TPayload {
  return {
    ...payload,
    $version: payload.$version ?? version,
  }
}

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
        const payloadWithVersion = withPayloadVersion(
          message.payload as (typeof message)['payload'],
          this.version
        )
        const sendOptions = (this.getSendOptions(payloadWithVersion) as PgBoss.JobInsert) || {}

        if (payloadWithVersion.scheduleAt) {
          sendOptions.startAfter = new Date(payloadWithVersion.scheduleAt)
        }

        return {
          ...sendOptions,
          name: this.getQueueName(),
          data: payloadWithVersion,
          deadLetter: this.deadLetterQueueName(),
        }
      })
    )
  }

  static send<T extends Event<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>,
    opts?: SendOptions & { tnx?: Knex }
  ) {
    const that = new this(withPayloadVersion(payload as T['payload'], this.version))
    return that.send(opts)
  }

  static invoke<T extends Event<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>
  ) {
    const that = new this(withPayloadVersion(payload as T['payload'], this.version))
    return that.invoke()
  }

  static invokeOrSend<T extends Event<any>>(
    this: StaticThis<T>,
    payload: Omit<T['payload'], '$version'>,
    options?: SendOptions & { sendWhenError?: (error: unknown) => boolean }
  ) {
    const that = new this(withPayloadVersion(payload as T['payload'], this.version))
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
      const tenant = await getTenantConfig(payload.tenant.ref, { reqId: payload.reqId })
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
      `DELETE FROM ${PG_BOSS_SCHEMA}.job
       WHERE id = $1
       AND EXISTS(
          SELECT 1 FROM ${PG_BOSS_SCHEMA}.job
             WHERE id != $2
             AND state < 'active'
             AND name = $3
             AND singleton_key = $4
       )
      `,
      [jobId, jobId, queueName, singletonKey]
    )
  }

  async invokeOrSend(
    sendOptions?: SendOptions & { sendWhenError?: (error: unknown) => boolean }
  ): Promise<string | void | null> {
    const eventClass = this.constructor as typeof Event

    if (!eventClass.allowSync) {
      throw ERRORS.InternalError(undefined, 'Cannot send this event synchronously')
    }

    try {
      await this.invoke()
    } catch (e) {
      if (sendOptions?.sendWhenError && !sendOptions.sendWhenError(e)) {
        throw e
      }
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
    const eventClass = this.constructor as typeof Event

    if (!eventClass.allowSync) {
      throw ERRORS.InternalError(undefined, 'Cannot send this event synchronously')
    }

    await eventClass.handle({
      id: '__sync',
      expireInSeconds: 0,
      name: eventClass.getQueueName(),
      data: {
        region,
        ...this.payload,
        $version: eventClass.version,
      },
    })
  }

  async send(customSendOptions?: SendOptions & { tnx?: Knex }): Promise<string | void | null> {
    const eventClass = this.constructor as typeof Event

    const shouldSend = await eventClass.shouldSend(this.payload)

    if (!shouldSend) {
      return
    }

    if (!pgQueueEnable) {
      if (eventClass.allowSync) {
        return eventClass.handle({
          id: '__sync',
          expireInSeconds: 0,
          name: eventClass.getQueueName(),
          data: {
            region,
            ...this.payload,
            $version: eventClass.version,
          },
        })
      } else {
        logger.warn(
          {
            type: 'queue',
            eventType: eventClass.eventName(),
          },
          '[Queue] skipped sending message'
        )
        return
      }
    }

    const startTime = process.hrtime.bigint()
    const sendOptions = eventClass.getSendOptions(this.payload) || {}

    if (this.payload.scheduleAt) {
      sendOptions.startAfter = new Date(this.payload.scheduleAt)
    }

    sendOptions!.deadLetter = eventClass.deadLetterQueueName()

    try {
      const queue = customSendOptions?.tnx
        ? Queue.createPgBoss({
            enableWorkers: false,
            db: new KnexQueueDB(customSendOptions.tnx),
          })
        : Queue.getInstance()

      const res = await queue.send({
        name: eventClass.getQueueName(),
        data: {
          region,
          ...this.payload,
          $version: eventClass.version,
        },
        options: {
          ...sendOptions,
          ...customSendOptions,
        },
      })

      queueJobScheduled.add(1, {
        name: eventClass.getQueueName(),
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

      if (!eventClass.allowSync) {
        throw e
      }

      return eventClass.handle({
        id: '__sync',
        expireInSeconds: 0,
        name: eventClass.getQueueName(),
        data: {
          region,
          ...this.payload,
          $version: eventClass.version,
        },
      })
    } finally {
      const duration = Number(process.hrtime.bigint() - startTime) / 1e9
      queueJobSchedulingTime.record(duration, {
        name: eventClass.getQueueName(),
      })
    }
  }
}
