import { randomBytes } from 'node:crypto'
import type { PoolClient, QueryResultRow } from 'pg'
import type { DatabaseConfig } from './config.js'
import { DatabaseWattError } from './errors.js'
import { isConnectionStateError, runQuery, toQueryResponse } from './pools.js'
import type { DestinationConfig, QueryResponse } from './types.js'

type LockRecord = {
  busy: Promise<unknown>
  client: PoolClient
  createdAt: number
  destination: DestinationConfig
  inTransaction: boolean
  lastUsedAt: number
  lockId: string
  terminal: boolean
}

export class LockRegistry {
  private readonly config: DatabaseConfig
  private readonly locks = new Map<string, LockRecord>()
  private readonly cleanupInterval: NodeJS.Timeout

  constructor(config: DatabaseConfig) {
    this.config = config
    this.cleanupInterval = setInterval(
      () => {
        void this.expireLocks()
      },
      Math.min(config.lockIdleTimeoutMs, config.lockMaxLifetimeMs, 10_000)
    )
    this.cleanupInterval.unref()
  }

  create(destination: DestinationConfig, client: PoolClient, inTransaction = false): string {
    const lockId = randomBytes(32).toString('base64url')
    const now = Date.now()
    this.locks.set(lockId, {
      busy: Promise.resolve(),
      client,
      createdAt: now,
      destination,
      inTransaction,
      lastUsedAt: now,
      lockId,
      terminal: false,
    })
    return lockId
  }

  getDestination(lockId: string): string | undefined {
    return this.locks.get(lockId)?.destination.id
  }

  getClient(lockId: string): PoolClient {
    return this.getLock(lockId).client
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    lockId: string,
    sql: string,
    values?: unknown[]
  ): Promise<QueryResponse<T>> {
    return this.withLock(lockId, async (lock) => {
      const result = await runQuery<T>(lock.client, sql, values)
      return toQueryResponse(result)
    })
  }

  async release(lockId: string): Promise<void> {
    await this.withLock(lockId, async (lock) => {
      if (lock.inTransaction) {
        throw new DatabaseWattError('PROTOCOL_ERROR', 'Cannot release a transaction lock')
      }

      this.remove(lock, undefined)
    })
  }

  async markTransaction(lockId: string): Promise<void> {
    await this.withLock(lockId, async (lock) => {
      lock.inTransaction = true
    })
  }

  async commit(lockId: string): Promise<void> {
    await this.withLock(lockId, async (lock) => {
      if (!lock.inTransaction) {
        throw new DatabaseWattError('PROTOCOL_ERROR', 'Lock is not in a transaction')
      }

      let releaseError: Error | undefined
      try {
        await runQuery(lock.client, 'COMMIT')
      } catch (error) {
        releaseError = error instanceof Error ? error : new Error(String(error))
        throw error
      } finally {
        this.remove(lock, releaseError)
      }
    })
  }

  async rollback(lockId: string): Promise<void> {
    await this.withLock(lockId, async (lock) => {
      if (!lock.inTransaction) {
        this.remove(lock, undefined)
        return
      }

      let releaseError: Error | undefined
      try {
        await runQuery(lock.client, 'ROLLBACK')
      } catch (error) {
        releaseError = error instanceof Error ? error : new Error(String(error))
        throw error
      } finally {
        this.remove(lock, releaseError)
      }
    })
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

    await Promise.allSettled(
      locks.map(async (lock) => {
        await lock.busy.catch(() => undefined)
        if (lock.inTransaction) {
          await runQuery(lock.client, 'ROLLBACK').catch(() => undefined)
        }
        lock.client.release()
      })
    )
  }

  private async withLock<T>(lockId: string, fn: (lock: LockRecord) => Promise<T>): Promise<T> {
    const lock = this.getLock(lockId)
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

  private getLock(lockId: string): LockRecord {
    const lock = this.locks.get(lockId)
    if (!lock || lock.terminal) {
      throw new DatabaseWattError('PROTOCOL_ERROR', 'Unknown lock ID')
    }
    return lock
  }

  private assertUsable(lock: LockRecord): void {
    const now = Date.now()
    if (now - lock.createdAt > this.config.lockMaxLifetimeMs) {
      this.remove(lock, new Error('Lock maximum lifetime exceeded'))
      throw new DatabaseWattError('PROTOCOL_ERROR', 'Unknown lock ID')
    }

    if (now - lock.lastUsedAt > this.config.lockIdleTimeoutMs) {
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
        now - lock.createdAt > this.config.lockMaxLifetimeMs ||
        now - lock.lastUsedAt > this.config.lockIdleTimeoutMs
      )
    })

    await Promise.allSettled(
      expired.map(async (lock) => {
        await lock.busy.catch(() => undefined)
        if (lock.inTransaction) {
          await runQuery(lock.client, 'ROLLBACK').catch(() => undefined)
        }
        this.remove(lock, undefined)
      })
    )
  }
}
