import { isConnectionStateError } from '@internal/database/postgres/pool-errors'
import hyperid from 'hyperid'
import type { PoolClient } from 'pg'
import type { StorageConfigType } from '../../config.js'
import { DatabaseWattError } from './errors.js'
import { runQuery, toQueryResponse } from './pools.js'
import type { DatabasePoolTarget, RawQueryResponse } from './protocol.js'

type LockRecord = {
  busy: Promise<unknown>
  client: PoolClient
  createdAt: number
  destination: DatabasePoolTarget
  inTransaction: boolean
  lastUsedAt: number
  lockId: string
  owner: object
  terminal: boolean
}

export class LockRegistry {
  private readonly config: StorageConfigType
  private readonly locks = new Map<string, LockRecord>()
  private readonly cleanupInterval: NodeJS.Timeout
  // Lock IDs come from hyperid, a random UUID per registry plus a counter: unique across
  // restarts of the Database application without generating random bytes for every
  // transaction. They are not secrets: a lock can only be used through the channel that
  // created it.
  private readonly nextLockId = hyperid()

  constructor(config: StorageConfigType) {
    this.config = config
    this.cleanupInterval = setInterval(
      () => {
        void this.expireLocks()
      },
      Math.min(config.databaseWattLockIdleTimeout, config.databaseWattLockMaxLifetime, 10_000)
    )
    this.cleanupInterval.unref()
  }

  create(
    destination: DatabasePoolTarget,
    client: PoolClient,
    inTransaction: boolean,
    owner: object
  ): string {
    const lockId = this.nextLockId()
    const now = Date.now()
    this.locks.set(lockId, {
      busy: Promise.resolve(),
      client,
      createdAt: now,
      destination,
      inTransaction,
      lastUsedAt: now,
      lockId,
      owner,
      terminal: false,
    })
    return lockId
  }

  /**
   * Rolls back and releases every lock created on behalf of `owner` (the
   * messaging channel of a storage worker). A worker that exits mid-transaction
   * would otherwise keep its connection, and any row locks, until the lock idle
   * timeout.
   */
  async releaseOwner(owner: object): Promise<number> {
    const owned = [...this.locks.values()].filter((lock) => lock.owner === owner)
    await Promise.allSettled(owned.map((lock) => this.discard(lock)))
    return owned.length
  }

  has(lockId: string): boolean {
    const lock = this.locks.get(lockId)
    return Boolean(lock && !lock.terminal)
  }

  getDestination(lockId: string): string | undefined {
    return this.locks.get(lockId)?.destination.id
  }

  getClient(lockId: string, owner: object | undefined): PoolClient {
    return this.getLock(lockId, owner).client
  }

  async query(
    lockId: string,
    sql: string,
    values: unknown[] | undefined,
    owner: object | undefined
  ): Promise<RawQueryResponse> {
    return this.withLock(
      lockId,
      async (lock) => {
        const result = await runQuery(lock.client, sql, values)
        return toQueryResponse(result)
      },
      owner
    )
  }

  async release(lockId: string, owner: object | undefined): Promise<void> {
    await this.withLock(
      lockId,
      async (lock) => {
        if (lock.inTransaction) {
          throw new DatabaseWattError('PROTOCOL_ERROR', 'Cannot release a transaction lock')
        }

        this.remove(lock, undefined)
      },
      owner
    )
  }

  async commit(lockId: string, owner: object | undefined): Promise<void> {
    await this.withLock(
      lockId,
      async (lock) => {
        if (!lock.inTransaction) {
          throw new DatabaseWattError('PROTOCOL_ERROR', 'Lock is not in a transaction')
        }

        let releaseError: Error | undefined
        try {
          await lock.client.query('COMMIT')
        } catch (error) {
          releaseError = error instanceof Error ? error : new Error(String(error))
          throw error
        } finally {
          this.remove(lock, releaseError)
        }
      },
      owner
    )
  }

  async rollback(lockId: string, owner: object | undefined): Promise<void> {
    await this.withLock(
      lockId,
      async (lock) => {
        if (!lock.inTransaction) {
          this.remove(lock, undefined)
          return
        }

        let releaseError: Error | undefined
        try {
          await lock.client.query('ROLLBACK')
        } catch (error) {
          releaseError = error instanceof Error ? error : new Error(String(error))
          throw error
        } finally {
          this.remove(lock, releaseError)
        }
      },
      owner
    )
  }

  async purge(lockId: string, releaseError?: Error): Promise<void> {
    const lock = this.locks.get(lockId)
    if (!lock) {
      return
    }

    await lock.busy.catch(() => undefined)
    this.remove(lock, releaseError)
  }

  async close(): Promise<void> {
    clearInterval(this.cleanupInterval)
    const locks = [...this.locks.values()]
    this.locks.clear()

    await Promise.allSettled(locks.map((lock) => this.discard(lock)))
  }

  private async discard(lock: LockRecord): Promise<void> {
    await lock.busy.catch(() => undefined)
    if (lock.terminal) {
      return
    }

    let releaseError: Error | undefined
    if (lock.inTransaction) {
      releaseError = await lock.client.query('ROLLBACK').then(
        () => undefined,
        (error) => (error instanceof Error ? error : new Error(String(error)))
      )
    }
    this.remove(lock, releaseError)
  }

  private async withLock<T>(
    lockId: string,
    fn: (lock: LockRecord) => Promise<T>,
    owner: object | undefined
  ): Promise<T> {
    const lock = this.getLock(lockId, owner)
    const work = lock.busy.then(async () => {
      this.assertUsable(lock)
      lock.lastUsedAt = Date.now()

      try {
        return await fn(lock)
      } catch (error) {
        if (isConnectionStateError(error)) {
          this.remove(lock, error instanceof Error ? error : new Error(String(error)))
        }
        throw error
      }
    })

    lock.busy = work.catch(() => undefined)
    return work
  }

  // A lock can only be used through the messaging channel that created it, so one storage
  // worker cannot reach into another worker's transaction. A request without a channel
  // can use no lock.
  private getLock(lockId: string, owner: object | undefined): LockRecord {
    const lock = this.locks.get(lockId)
    if (!lock || lock.terminal || owner === undefined || lock.owner !== owner) {
      throw new DatabaseWattError('PROTOCOL_ERROR', 'Unknown lock ID')
    }
    return lock
  }

  private assertUsable(lock: LockRecord): void {
    const now = Date.now()
    if (now - lock.createdAt > this.config.databaseWattLockMaxLifetime) {
      this.remove(lock, new Error('Lock maximum lifetime exceeded'))
      throw new DatabaseWattError('PROTOCOL_ERROR', 'Unknown lock ID')
    }

    if (now - lock.lastUsedAt > this.config.databaseWattLockIdleTimeout) {
      this.remove(lock, new Error('Lock idle timeout exceeded'))
      throw new DatabaseWattError('PROTOCOL_ERROR', 'Unknown lock ID')
    }
  }

  private remove(lock: LockRecord, releaseError: Error | undefined): void {
    if (lock.terminal) {
      return
    }

    lock.terminal = true
    this.locks.delete(lock.lockId)
    lock.client.release(releaseError)
  }

  private async expireLocks(): Promise<void> {
    const now = Date.now()
    const expired = [...this.locks.values()].filter((lock) => {
      return (
        now - lock.createdAt > this.config.databaseWattLockMaxLifetime ||
        now - lock.lastUsedAt > this.config.databaseWattLockIdleTimeout
      )
    })

    await Promise.allSettled(expired.map((lock) => this.discard(lock)))
  }
}
