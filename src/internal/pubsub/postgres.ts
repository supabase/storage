import EventEmitter from 'node:events'
import { setTimeout as sleep } from 'node:timers/promises'
import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import createSubscriber, { Subscriber } from 'pg-listen'
import { PubSubAdapter } from './adapter'

export class PostgresPubSub extends EventEmitter implements PubSubAdapter {
  isConnected = false
  subscriber: Subscriber
  private readonly notifications = new EventEmitter().setMaxListeners(0)
  private readonly recoveryAbort = new AbortController()
  private recovery?: Promise<void>
  private closed = false
  private connectionGeneration = 0

  constructor(
    private readonly connectionString: string,
    private readonly reconnect: {
      baseDelayMs?: number
      maxDelayMs?: number
      connectionTimeoutMs?: number
    } = {}
  ) {
    super()
    this.subscriber = this.createSubscriber()
  }

  private createSubscriber(connectionTimeoutMillis?: number): Subscriber {
    const subscriber = createSubscriber(
      {
        connectionString: this.connectionString,
        connectionTimeoutMillis,
        query_timeout: 10_000,
      },
      {
        // pg-listen loops while attempt < retryLimit; the wrapper owns all retries.
        retryLimit: 1,
      }
    )
    // Keep listeners stable when a failed subscriber must be replaced.
    subscriber.events.on('notification', ({ channel, payload }) => {
      if (this.subscriber === subscriber && !this.closed) {
        this.notifications.emit(channel, payload)
      }
    })
    subscriber.notifications = this.notifications as unknown as Subscriber['notifications']

    let established = false
    subscriber.events.on('connected', () => {
      established = true
    })
    subscriber.events.on('error', (error) => {
      if (this.subscriber !== subscriber || this.closed || !established) return
      // pg-listen emits other errors, such as malformed notification payloads.
      if (error.message.startsWith('Re-initializing the PostgreSQL notification client')) {
        this.connectionGeneration++
        this.isConnected = false
        if (!this.recovery) {
          logSchema.warning(logger, '[PubSub] Connection lost; reconnecting', { type: 'pubsub' })
          this.recovery = this.recover()
        }
      }
      this.emit('error', error)
    })
    return subscriber
  }

  private async listenAll(subscriber: Subscriber, generation: number): Promise<void> {
    for (;;) {
      if (this.closed || this.connectionGeneration !== generation) {
        throw new Error('PubSub connection changed while restoring subscriptions')
      }
      const pending = this.notifications
        .eventNames()
        .map((channel) => subscriber.listenTo(channel as string))
        .filter(Boolean)
      if (pending.length === 0) {
        this.isConnected = true
        return
      }
      await Promise.all(pending)
    }
  }

  private async recover(): Promise<void> {
    const { baseDelayMs = 100, maxDelayMs = 10_000 } = this.reconnect
    const startedAt = Date.now()
    let lastWarnAt = 0
    await this.subscriber.close().catch(() => {})
    for (let attempt = 0; !this.closed; attempt++) {
      const delay = Math.random() * Math.min(baseDelayMs * 2 ** attempt, maxDelayMs)
      try {
        await sleep(delay, undefined, { signal: this.recoveryAbort.signal })
      } catch {
        return
      }
      if (this.closed) return

      const subscriber = this.createSubscriber(this.reconnect.connectionTimeoutMs ?? 3000)
      this.subscriber = subscriber
      const generation = this.connectionGeneration
      try {
        await subscriber.connect()
        await this.listenAll(subscriber, generation)
        if (this.closed || this.connectionGeneration !== generation) {
          throw new Error('PubSub connection changed while restoring subscriptions')
        }
        this.recovery = undefined
        logSchema.info(logger, '[PubSub] Subscriptions restored', { type: 'pubsub' })
        return
      } catch (error) {
        this.isConnected = false
        await subscriber.close().catch(() => {})
        if (!this.closed && Date.now() - lastWarnAt >= 60_000) {
          lastWarnAt = Date.now()
          logSchema.warning(logger, '[PubSub] Recovery failed; retrying', {
            type: 'pubsub',
            error,
            metadata: JSON.stringify({ attempt: attempt + 1, downForMs: lastWarnAt - startedAt }),
          })
        }
      }
    }
  }

  async start(opts?: { signal?: AbortSignal }): Promise<void> {
    if (opts?.signal?.aborted) {
      throw ERRORS.Aborted('Postgres pubsub connection aborted')
    }

    const subscriber = this.subscriber
    const generation = this.connectionGeneration
    await subscriber.connect()

    if (opts?.signal) {
      opts.signal.addEventListener(
        'abort',
        async () => {
          logSchema.info(logger, '[PubSub] Stopping', {
            type: 'pubsub',
          })
          await this.close()
        },
        { once: true }
      )
    }

    await this.listenAll(subscriber, generation)
  }

  async close(): Promise<void> {
    this.closed = true
    this.isConnected = false
    this.recoveryAbort.abort()
    this.notifications.removeAllListeners()
    await this.subscriber.close()
    logSchema.info(logger, '[PubSub] Exited', {
      type: 'pubsub',
    })
  }

  async publish(channel: string, payload: unknown): Promise<void> {
    await this.subscriber.notify(channel, payload)
  }

  async subscribe(channel: string, cb: (payload: unknown) => void): Promise<void> {
    const listenerCount = this.notifications.listenerCount(channel)
    this.notifications.on(channel, cb)

    if (this.isConnected && listenerCount === 0) {
      await this.subscriber.listenTo(channel)
    }
  }

  async unsubscribe(channel: string, cb: (payload: unknown) => void): Promise<void> {
    this.notifications.removeListener(channel, cb)

    const isListening = this.notifications.listenerCount(channel) > 0

    if (!isListening && this.isConnected) {
      await this.subscriber.unlisten(channel)
    }
  }
}
