import { Lock, Locker, RequestRelease } from '@tus/server'
import { clearTimeout } from 'timers'
import EventEmitter from 'events'
import { Database, DBError } from '../database'
import { PubSubAdapter } from '../../pubsub'
import { UploadId } from './upload-id'

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

  removeListeners(id: string) {
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
          const acquired = await Promise.race([
            this.waitTimeout(15000, abortController.signal),
            this.acquireLock(db, this.id, abortController.signal),
          ])

          abortController.abort()

          if (!acquired) {
            throw new DBError('acquiring lock timeout', 503, 'acquiring_lock_timeout')
          }

          await new Promise<void>((innerResolve) => {
            this.tnxResolver = innerResolve
            resolve()
          })
        })
        .catch(reject)
    })

    this.notifier.onRelease(this.id, () => {
      cancelReq()
    })
  }

  async unlock(): Promise<void> {
    this.notifier.removeListeners(this.id)
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
        if (e instanceof DBError && e.message === 'resource_locked') {
          await this.notifier.release(id)
          await new Promise((resolve) => {
            setTimeout(resolve, 100)
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
      signal.addEventListener('abort', onAbort)
    })
  }
}
