import EventEmitter from 'node:events'
import { clearTimeout } from 'node:timers'
import { ERRORS, ErrorCode, StorageBackendError } from '@internal/errors'
import { PubSubAdapter } from '@internal/pubsub'
import { Lock, Locker, RequestRelease } from '@tus/server'
import { Database } from '../../database'
import { UploadId } from './upload-id'

const REQUEST_LOCK_RELEASE_MESSAGE = 'REQUEST_LOCK_RELEASE'

function isRequestLockReleaseMessage(payload: unknown): payload is { id: string } {
  if (!payload || typeof payload !== 'object') {
    return false
  }

  return typeof (payload as { id?: unknown }).id === 'string'
}

export class LockNotifier {
  protected events = new EventEmitter()

  handler = (payload: unknown) => {
    if (!isRequestLockReleaseMessage(payload)) {
      return
    }

    this.events.emit(`release:${payload.id}`)
  }

  constructor(private readonly pubSub: PubSubAdapter) {}

  release(id: string) {
    return this.pubSub.publish(REQUEST_LOCK_RELEASE_MESSAGE, { id })
  }

  onRelease(id: string, callback: () => void) {
    this.events.once(`release:${id}`, callback)
  }

  unsubscribe(id: string) {
    this.events.removeAllListeners(`release:${id}`)
  }

  async start() {
    await this.pubSub.subscribe(REQUEST_LOCK_RELEASE_MESSAGE, this.handler)
  }

  stop() {
    return this.pubSub.unsubscribe(REQUEST_LOCK_RELEASE_MESSAGE, this.handler)
  }
}

export class PgLocker implements Locker {
  constructor(
    private readonly db: Database,
    private readonly notifier: LockNotifier
  ) {}

  newLock(id: string): Lock {
    return new PgLock(id, this.db, this.notifier)
  }
}

export class PgLock implements Lock {
  private tnxResolver?: () => void
  private isLocked = false

  constructor(
    private readonly id: string,
    private readonly db: Database,
    private readonly notifier: LockNotifier
  ) {}

  async lock(stopSignal: AbortSignal, cancelReq: RequestRelease): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.holdLockTransaction(stopSignal, resolve).catch(reject)
    })

    this.notifier.onRelease(this.id, () => cancelReq())
  }

  async unlock(): Promise<void> {
    if (!this.isLocked) {
      return
    }

    this.isLocked = false
    this.notifier.unsubscribe(this.id)

    if (this.tnxResolver) {
      const resolver = this.tnxResolver
      this.tnxResolver = undefined
      resolver()
    }
  }

  protected async holdLockTransaction(stopSignal: AbortSignal, resolve: () => void) {
    const abortController = new AbortController()
    const uploadId = UploadId.fromString(this.id)
    const onAbort = () => abortController.abort()

    try {
      stopSignal.addEventListener('abort', onAbort)

      const acquired = await Promise.race([
        this.waitTimeout(5000, abortController.signal),
        (async () => {
          while (!abortController.signal.aborted) {
            try {
              await this.db.acquireObjectLockForTransaction(
                uploadId.bucket,
                uploadId.objectName,
                uploadId.version,
                async () => {
                  this.isLocked = true
                  await new Promise<void>((innerResolve) => {
                    this.tnxResolver = innerResolve
                    resolve()
                  })
                },
                { timeout: 5 * 60 * 1000 }
              )
              return true
            } catch (e) {
              if (e instanceof StorageBackendError && e.code === ErrorCode.ResourceLocked) {
                await this.notifier.release(this.id)
                await new Promise((resolve) => {
                  const timeoutId = setTimeout(resolve, 500)
                  const cleanup = () => {
                    clearTimeout(timeoutId)
                    abortController.signal.removeEventListener('abort', cleanup)
                  }
                  abortController.signal.addEventListener('abort', cleanup, { once: true })
                })
                continue
              }
              throw e
            }
          }
          return false
        })(),
      ])

      if (!acquired) {
        throw ERRORS.LockTimeout()
      }
    } finally {
      abortController.abort()
      stopSignal.removeEventListener('abort', onAbort)
    }
  }

  protected waitTimeout(timeout: number, signal: AbortSignal) {
    return new Promise((resolve) => {
      const timeoutId = setTimeout(() => {
        resolve(false)
      }, timeout)
      const onAbort = () => {
        clearTimeout(timeoutId)
        signal.removeEventListener('abort', onAbort)
      }
      signal.addEventListener('abort', onAbort, { once: true })
    })
  }
}
