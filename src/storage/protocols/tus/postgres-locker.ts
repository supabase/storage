import { Lock, Locker, RequestRelease } from '@tus/server'
import { clearTimeout } from 'node:timers'
import EventEmitter from 'node:events'
import { Database } from '../../database'
import { PubSubAdapter } from '@internal/pubsub'
import { UploadId } from './upload-id'
import { ErrorCode, ERRORS, StorageBackendError } from '@internal/errors'

const REQUEST_LOCK_RELEASE_MESSAGE = 'REQUEST_LOCK_RELEASE'

export class LockNotifier {
  protected events = new EventEmitter()
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

  async subscribe() {
    await this.pubSub.subscribe(REQUEST_LOCK_RELEASE_MESSAGE, ({ id }) => {
      this.events.emit(`release:${id}`)
    })
  }
}

export class PgLocker implements Locker {
  constructor(private readonly db: Database, private readonly notifier: LockNotifier) {}

  newLock(id: string): Lock {
    return new PgLock(id, this.db, this.notifier)
  }
}

export class PgLock implements Lock {
  tnxResolver?: () => void

  constructor(
    private readonly id: string,
    private readonly db: Database,
    private readonly notifier: LockNotifier
  ) {}

  async lock(cancelReq: RequestRelease): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.db
        .withTransaction(async (db) => {
          const abortController = new AbortController()

          try {
            const acquired = await Promise.race([
              this.waitTimeout(5000, abortController.signal),
              this.acquireLock(db, this.id, abortController.signal),
            ])

            if (!acquired) {
              throw ERRORS.LockTimeout()
            }

            await new Promise<void>((innerResolve) => {
              this.tnxResolver = innerResolve
              resolve()
            })
          } finally {
            abortController.abort()
          }
        })
        .catch(reject)
    })

    this.notifier.onRelease(this.id, () => {
      cancelReq()
    })
  }

  async unlock(): Promise<void> {
    this.notifier.unsubscribe(this.id)
    this.tnxResolver?.()
    this.tnxResolver = undefined
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
            setTimeout(resolve, 500)
          })
          continue
        }
        throw e
      }
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
