import { PubSubAdapter } from '@internal/pubsub'
import { LockNotifier } from './postgres-locker'

class FakePubSub implements PubSubAdapter {
  readonly startSpy = vi.fn(async () => undefined)
  readonly publishSpy = vi.fn(async (_channel: string, _message: unknown) => undefined)
  readonly subscribeSpy = vi.fn(
    async (_channel: string, _cb: (message: unknown) => void) => undefined
  )
  readonly unsubscribeSpy = vi.fn(
    async (_channel: string, _cb: (message: unknown) => void) => undefined
  )
  readonly closeSpy = vi.fn(async () => undefined)

  start(): Promise<void> {
    return this.startSpy()
  }

  publish(channel: string, message: unknown): Promise<void> {
    return this.publishSpy(channel, message)
  }

  subscribe(channel: string, cb: (message: unknown) => void): Promise<void> {
    return this.subscribeSpy(channel, cb)
  }

  unsubscribe(channel: string, cb: (message: unknown) => void): Promise<void> {
    return this.unsubscribeSpy(channel, cb)
  }

  close(): Promise<void> {
    return this.closeSpy()
  }

  on(): this {
    return this
  }
}

describe('LockNotifier', () => {
  it('ignores malformed pubsub payloads', () => {
    const pubSub = new FakePubSub()
    const notifier = new LockNotifier(pubSub)
    const onRelease = vi.fn()

    notifier.onRelease('upload-id', onRelease)

    expect(() => notifier.handler('upload-id')).not.toThrow()
    expect(() => notifier.handler({ id: 123 })).not.toThrow()
    expect(onRelease).not.toHaveBeenCalled()
  })

  it('emits release events for valid payloads', () => {
    const pubSub = new FakePubSub()
    const notifier = new LockNotifier(pubSub)
    const onRelease = vi.fn()

    notifier.onRelease('upload-id', onRelease)
    notifier.handler({ id: 'upload-id' })

    expect(onRelease).toHaveBeenCalledTimes(1)
  })

  it('subscribes and unsubscribes the shared handler', async () => {
    const pubSub = new FakePubSub()
    const notifier = new LockNotifier(pubSub)

    await notifier.start()
    await notifier.stop()

    expect(pubSub.subscribeSpy).toHaveBeenCalledWith('REQUEST_LOCK_RELEASE', notifier.handler)
    expect(pubSub.unsubscribeSpy).toHaveBeenCalledWith('REQUEST_LOCK_RELEASE', notifier.handler)
  })
})
