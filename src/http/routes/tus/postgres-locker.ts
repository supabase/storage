import { Locker } from '@tus/server'
import { Database, DBError } from '../../../storage/database'
import { UploadId } from './upload-id'
import http from 'http'

class Lock {
  tnxResolver?: () => void
  wait?: Promise<void>
}

interface PostgresLockerOptions {
  shouldWait: boolean
  trace?: string
  req?: http.IncomingMessage
}

export class PostgresLocker implements Locker {
  private locks: Map<string, Lock> = new Map()

  constructor(private readonly db: Database, public readonly options?: PostgresLockerOptions) {}

  async lock(id: string): Promise<void> {
    const uploadId = UploadId.fromString(id)

    const lock = new Lock()

    await new Promise<void>((resolve, reject) => {
      const pendingPromise = this.db
        .withTransaction(async (db) => {
          try {
            if (this.options?.shouldWait) {
              await db.waitObjectLock(uploadId.bucket, uploadId.objectName, uploadId.version)
            } else {
              await db.mustLockObject(uploadId.bucket, uploadId.objectName, uploadId.version)
            }

            await new Promise<void>((innerResolve) => {
              lock.tnxResolver = innerResolve
              lock.wait = pendingPromise as Promise<void>
              this.locks.set(id, lock)
              resolve()
            })
          } catch (e) {
            if (e instanceof DBError && e.message === 'resource_locked') {
              throw {
                status_code: 409,
                body: 'Resource already locked',
              }
            } else {
              throw e
            }
          }
        })
        .catch(reject)
    })
  }

  async unlock(id: string): Promise<void> {
    const lock = this.locks.get(id)
    if (!lock) {
      throw new Error('unlocking not existing lock')
    }

    this.locks.delete(id)
    lock.tnxResolver && lock.tnxResolver()

    if (lock.wait) {
      await lock.wait
    }
  }
}
