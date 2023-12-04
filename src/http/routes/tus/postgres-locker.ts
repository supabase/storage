import { Locker } from '@tus/server'
import { Database, DBError } from '../../../storage/database'
import { PubSubAdapter } from '../../../pubsub'
import { UploadId } from './upload-id'
import { RequestRelease } from '@tus/server/models/Locker'
import { clearTimeout } from 'timers'

class Lock {
  tnxResolver?: () => void
  requestRelease?: RequestRelease
}

const REQUEST_LOCK_RELEASE_MESSAGE = 'REQUEST_LOCK_RELEASE'

export class LockManager {
  readonly listener: Promise<void>
  private locks: Map<string, Lock> = new Map()

  constructor(private readonly pubSub: PubSubAdapter) {
    this.listener = this.listenForMessages()
  }

  releaseExistingLock(id: string) {
    return this.pubSub.publish(REQUEST_LOCK_RELEASE_MESSAGE, { id })
  }

  addLock(id: string, lock: Lock) {
    this.locks.set(id, lock)
  }

  deleteLock(id: string) {
    const lock = this.locks.get(id)
    if (!lock) {
      throw new Error('unlocking not existing lock')
    }

    lock.tnxResolver && lock.tnxResolver()
    this.locks.delete(id)
  }

  protected async listenForMessages() {
    await this.pubSub.subscribe(REQUEST_LOCK_RELEASE_MESSAGE, async ({ id }: { id: string }) => {
      const lock = this.locks.get(id)
      if (lock) {
        await lock.requestRelease?.()
      }
    })
  }
}

export class PostgresLocker implements Locker {
  constructor(private readonly manager: LockManager, private readonly db: Database) {}

  async lock(id: string, cancel: RequestRelease): Promise<void> {
    await this.manager.listener

    await new Promise<void>((resolve, reject) => {
      this.db
        .withTransaction(async (db) => {
          const abortController = new AbortController()
          const acquired = await Promise.race([
            this.waitTimeout(15000, abortController.signal),
            this.acquireLock(db, id, abortController.signal),
          ])

          abortController.abort()

          if (!acquired) {
            throw new DBError('acquiring lock timeout', 503, 'acquiring_lock_timeout')
          }

          await new Promise<void>((innerResolve) => {
            const lock = new Lock()
            lock.tnxResolver = innerResolve
            lock.requestRelease = cancel
            this.manager.addLock(id, lock)
            resolve()
          })
        })
        .catch(reject)
    })
  }

  async unlock(id: string): Promise<void> {
    this.manager.deleteLock(id)
  }

  protected async acquireLock(db: Database, id: string, signal: AbortSignal) {
    const uploadId = UploadId.fromString(id)

    while (!signal.aborted) {
      try {
        await db.mustLockObject(uploadId.bucket, uploadId.objectName, uploadId.version)
        return true
      } catch (e) {
        if (e instanceof DBError && e.message === 'resource_locked') {
          await this.manager.releaseExistingLock(id)
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
