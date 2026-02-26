import EventEmitter from 'node:events'
import { clearTimeout } from 'node:timers'
import { ERRORS, ErrorCode, StorageBackendError } from '@internal/errors'
import { PubSubAdapter } from '@internal/pubsub'
import { Lock, Locker, RequestRelease } from '@tus/server'
import { Database } from '../../database'
import { UploadId } from './upload-id'

const REQUEST_LOCK_RELEASE_MESSAGE = 'REQUEST_LOCK_RELEASE'

export class LockNotifier {
  protected events = new EventEmitter()

  handler = ({ id }: { id: string }) => {
    this.events.emit(`release:${id}`)
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
      this.db
        .withTransaction(
          async (db) => {
            const abortController = new AbortController()
            let onAbort: (() => void) | undefined

            try {
              onAbort = () => {
                abortController.abort()
              }
              stopSignal.addEventListener('abort', onAbort)

              const acquired = await Promise.race([
                this.waitTimeout(5000, abortController.signal),
                this.acquireLock(db, this.id, abortController.signal),
              ])

              if (!acquired) {
                throw ERRORS.LockTimeout()
              }

              this.isLocked = true

              await new Promise<void>((innerResolve) => {
                this.tnxResolver = innerResolve
                resolve()
              })
            } finally {
              abortController.abort()
            }
          },
          {
            timeout: 5 * 60 * 1000, // 5 minutes
          }
        )
        .catch(reject)
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

  protected async acquireLock(db: Database, id: string, signal: AbortSignal) {
    const uploadId = UploadId.fromString(id)

    while (!signal.aborted) {
      try {
        await db.mustLockObject(uploadId.bucket, uploadId.objectName, uploadId.version)
        return true
      } catch (e) {
        if (e instanceof StorageBackendError && e.code === ErrorCode.ResourceLocked) {
          await this.notifier.release(id)
          await new Promise((resolve) => {
            const timeoutId = setTimeout(resolve, 500)
            const cleanup = () => {
              clearTimeout(timeoutId)
              signal.removeEventListener('abort', cleanup)
            }
            signal.addEventListener('abort', cleanup, { once: true })
          })
          continue
        }
        throw e
      }
    }

    return false
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
