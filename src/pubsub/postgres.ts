import createSubscriber, { Subscriber } from 'pg-listen'
import { PubSubAdapter } from './adapter'

export class PostgresPubSub implements PubSubAdapter {
  isConnected = false
  subscriber: Subscriber

  constructor(connectionString: string) {
    this.subscriber = createSubscriber({ connectionString })
  }

  async connect(): Promise<void> {
    await this.subscriber.connect()
    this.isConnected = true

    await Promise.all(
      this.subscriber.notifications.eventNames().map(async (channel) => {
        return this.subscriber.listenTo(channel as string)
      })
    )
  }

  close(): Promise<void> {
    this.subscriber.notifications.eventNames().forEach((event) => {
      this.subscriber.notifications.removeAllListeners(event)
    })
    return this.subscriber.close()
  }

  async publish(channel: string, payload: unknown): Promise<void> {
    await this.subscriber.notify(channel, payload)
  }

  async subscribe(channel: string, cb: (payload: any) => void): Promise<void> {
    const listenerCount = this.subscriber.notifications.listenerCount(channel)
    this.subscriber.notifications.on(channel, cb)

    if (this.isConnected && listenerCount === 0) {
      await this.subscriber.listenTo(channel)
    }
  }

  async unsubscribe(channel: string, cb: (payload: any) => void): Promise<void> {
    this.subscriber.notifications.removeListener(channel, cb)

    const isListening = this.subscriber.notifications.listenerCount(channel) > 0

    if (!isListening) {
      await this.subscriber.unlisten(channel)
    }
  }
}
