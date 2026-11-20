import { normalizeIsolationLevel } from '@internal/database/postgres/sql.js'
import { getMessaging } from '@platformatic/globals'
import { getConfig, type StorageConfigType } from '../../config.js'
import { CancellationRegistry } from './cancellation.js'
import { DatabaseWattError, type ErrorContext, toErrorResponse } from './errors.js'
import { LockRegistry } from './locks.js'
import { registerDatabaseWattMetrics } from './metrics.js'
import { PoolRegistry, runQuery } from './pools.js'
import {
  type AcquireConnectionRequest,
  type BeginTransactionRequest,
  type CancelRequest,
  type CommitTransactionRequest,
  DATABASE_MESSAGES,
  type LockedQueryRequest,
  type QueryRequest,
  type ReleaseConnectionRequest,
  type RollbackTransactionRequest,
} from './protocol.js'
import {
  validateCancelRequest,
  validateLockRequestEnvelope,
  validateNonLockRequestEnvelope,
  validateQueryEnvelope,
} from './validation.js'

export class Application {
  #config: StorageConfigType
  #pools: PoolRegistry
  #locks: LockRegistry
  #cancellations: CancellationRegistry
  #shuttingDown: boolean
  #stats: Record<string, number>

  constructor() {
    this.#config = getConfig()
    this.#pools = new PoolRegistry(this.#config)
    this.#locks = new LockRegistry(this.#config)
    this.#cancellations = new CancellationRegistry()
    this.#shuttingDown = false
    this.#stats = {
      acquire: 0,
      beginTransaction: 0,
      cancel: 0,
      commitTransaction: 0,
      lockedQuery: 0,
      query: 0,
      release: 0,
      rollbackTransaction: 0,
    }

    this.#registerHandlers()
    registerDatabaseWattMetrics(this.#pools)
  }

  close(): Promise<void> {
    this.#shuttingDown = true

    return this.#withShutdownTimeout(
      Promise.allSettled([this.#locks.close(), this.#pools.close()]).then(() => undefined),
      this.#config.databaseWattShutdownTimeout
    )
  }

  #registerHandlers(): void {
    const messaging = getMessaging({ throwOnMissing: false })

    if (!messaging) {
      return
    }

    messaging.handle(DATABASE_MESSAGES.query, this.#handleQuery.bind(this))
    messaging.handle(DATABASE_MESSAGES.acquire, this.#handleAcquire.bind(this))
    messaging.handle(DATABASE_MESSAGES.lockedQuery, this.#handleLockedQuery.bind(this))
    messaging.handle(DATABASE_MESSAGES.release, this.#handleRelease.bind(this))
    messaging.handle(DATABASE_MESSAGES.beginTransaction, this.#handleBeginTransaction.bind(this))
    messaging.handle(DATABASE_MESSAGES.commitTransaction, this.#handleCommitTransaction.bind(this))
    messaging.handle(
      DATABASE_MESSAGES.rollbackTransaction,
      this.#handleRollbackTransaction.bind(this)
    )
    messaging.handle(DATABASE_MESSAGES.cancel, this.#handleCancel.bind(this))
    messaging.handle('database.test.stats', () => ({ ...this.#stats }))
    messaging.handle('database.test.resetStats', this.#handleResetStats.bind(this))
  }

  async #handleQuery(rawRequest: unknown): Promise<unknown> {
    this.#stats.query++
    let request: QueryRequest | undefined
    let cancellationRequestId: string | undefined

    try {
      validateNonLockRequestEnvelope(rawRequest)
      validateQueryEnvelope(rawRequest)
      request = rawRequest as QueryRequest

      this.#assertAcceptingWork()
      this.#cancellations.start(request.requestId, { cancelled: false })
      cancellationRequestId = request.requestId

      const response = await this.#pools.query(
        request.destination,
        request.sql,
        request.values,
        (client) => this.#cancellations.setClient(request?.requestId, client)
      )
      return response
    } catch (error) {
      return toErrorResponse(error, request ? this.#withDestinationContext(request) : undefined)
    } finally {
      this.#cancellations.finish(cancellationRequestId)
    }
  }

  async #handleAcquire(rawRequest: unknown): Promise<unknown> {
    this.#stats.acquire++
    let request: AcquireConnectionRequest | undefined

    try {
      validateNonLockRequestEnvelope(rawRequest)
      request = rawRequest as AcquireConnectionRequest

      this.#assertAcceptingWork()
      const client = await this.#pools.acquire(request.destination)
      return { lockId: this.#locks.create(request.destination, client) }
    } catch (error) {
      return toErrorResponse(error, request ? this.#withDestinationContext(request) : undefined)
    }
  }

  async #handleLockedQuery(rawRequest: unknown): Promise<unknown> {
    this.#stats.lockedQuery++
    let request: LockedQueryRequest | undefined
    let context: LockedQueryRequest | (LockedQueryRequest & { destination?: string }) | undefined
    let cancellationRequestId: string | undefined

    try {
      validateLockRequestEnvelope(rawRequest)
      validateQueryEnvelope(rawRequest)
      request = rawRequest as LockedQueryRequest
      context = request
      context = this.#withLockContext(request)

      this.#assertAcceptingWork()
      this.#cancellations.start(request.requestId, { cancelled: false, lockId: request.lockId })
      cancellationRequestId = request.requestId

      this.#cancellations.setClient(request.requestId, this.#locks.getClient(request.lockId))
      const response = await this.#locks.query(request.lockId, request.sql, request.values)
      return response
    } catch (error) {
      return toErrorResponse(error, context ?? undefined)
    } finally {
      this.#cancellations.finish(cancellationRequestId)
    }
  }

  async #handleRelease(rawRequest: unknown): Promise<unknown> {
    this.#stats.release++
    let request: ReleaseConnectionRequest | undefined
    let context:
      | ReleaseConnectionRequest
      | (ReleaseConnectionRequest & { destination?: string })
      | undefined

    try {
      validateLockRequestEnvelope(rawRequest)
      request = rawRequest as ReleaseConnectionRequest
      context = request
      context = this.#withLockContext(request)

      await this.#locks.release(request.lockId)
      return { released: true }
    } catch (error) {
      return toErrorResponse(error, context ?? undefined)
    }
  }

  async #handleBeginTransaction(rawRequest: unknown): Promise<unknown> {
    this.#stats.beginTransaction++
    let request: BeginTransactionRequest | undefined

    try {
      validateNonLockRequestEnvelope(rawRequest)
      request = rawRequest as BeginTransactionRequest

      this.#assertAcceptingWork()
      const client = await this.#pools.acquire(request.destination)
      let lockId: string | undefined

      try {
        await runQuery(client, this.#buildBeginStatement(request))
        if (this.#config.databaseStatementTimeout > 0) {
          await runQuery(client, `SELECT set_config('statement_timeout', $1, true)`, [
            `${this.#config.databaseStatementTimeout}ms`,
          ])
        }
        lockId = this.#locks.create(request.destination, client, true)
        return { lockId }
      } catch (error) {
        client.release(error instanceof Error ? error : new Error(String(error)))
        throw error
      }
    } catch (error) {
      return toErrorResponse(error, request ? this.#withDestinationContext(request) : undefined)
    }
  }

  async #handleCommitTransaction(rawRequest: unknown): Promise<unknown> {
    this.#stats.commitTransaction++
    let request: CommitTransactionRequest | undefined
    let context:
      | CommitTransactionRequest
      | (CommitTransactionRequest & { destination?: string })
      | undefined

    try {
      validateLockRequestEnvelope(rawRequest)
      request = rawRequest as CommitTransactionRequest
      context = request
      context = this.#withLockContext(request)

      await this.#locks.commit(request.lockId)
      return { committed: true }
    } catch (error) {
      return toErrorResponse(error, context ?? undefined)
    }
  }

  async #handleRollbackTransaction(rawRequest: unknown): Promise<unknown> {
    this.#stats.rollbackTransaction++
    let request: RollbackTransactionRequest | undefined
    let context:
      | RollbackTransactionRequest
      | (RollbackTransactionRequest & { destination?: string })
      | undefined

    try {
      validateLockRequestEnvelope(rawRequest)
      request = rawRequest as RollbackTransactionRequest
      context = request
      context = this.#withLockContext(request)

      await this.#locks.rollback(request.lockId)
      return { rolledBack: true }
    } catch (error) {
      return toErrorResponse(error, context ?? undefined)
    }
  }

  async #handleCancel(rawRequest: unknown): Promise<unknown> {
    this.#stats.cancel++
    let request: CancelRequest | undefined

    try {
      validateCancelRequest(rawRequest)
      request = rawRequest as CancelRequest

      return this.#cancellations.cancel(request.requestId, request.lockId)
    } catch (error) {
      return toErrorResponse(error, request ?? undefined)
    }
  }

  #handleResetStats(): { reset: boolean } {
    for (const key of Object.keys(this.#stats)) {
      this.#stats[key] = 0
    }

    return { reset: true }
  }

  #withLockContext<T extends { lockId: string; operationName?: string; requestId?: string }>(
    request: T
  ): T & { destination?: string } {
    return {
      ...request,
      destination: this.#locks.getDestination(request.lockId),
    }
  }

  #withDestinationContext(request: QueryRequest | AcquireConnectionRequest): ErrorContext {
    return {
      destination: request.destination.id,
      operationName: request.operationName,
      requestId: request.requestId,
    }
  }

  #assertAcceptingWork(): void {
    if (this.#shuttingDown) {
      throw new DatabaseWattError('SHUTDOWN', 'Database application is shutting down')
    }
  }

  #buildBeginStatement(request: BeginTransactionRequest): string {
    const modes: string[] = []
    const isolationLevel = normalizeIsolationLevel(request.isolationLevel)

    if (isolationLevel) {
      modes.push(`ISOLATION LEVEL ${isolationLevel}`)
    }

    if (request.readOnly) {
      modes.push('READ ONLY')
    }

    if (modes.length === 0) {
      return 'BEGIN'
    }

    return `BEGIN ${modes.join(', ')}`
  }

  async #withShutdownTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
    let timeout: NodeJS.Timeout | undefined
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeout = setTimeout(() => {
        reject(new DatabaseWattError('SHUTDOWN', 'Database application shutdown timed out'))
      }, timeoutMs)
      timeout.unref()
    })

    try {
      return await Promise.race([promise, timeoutPromise])
    } finally {
      if (timeout) {
        clearTimeout(timeout)
      }
    }
  }
}
