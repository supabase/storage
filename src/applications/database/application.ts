import { isRetryableTransactionSetupError } from '@internal/database/postgres/pool-errors'
import {
  nextTransactionSetupRetryDelay,
  TRANSACTION_SETUP_RETRY,
} from '@internal/database/postgres/retry'
import { buildBeginStatement } from '@internal/database/postgres/sql'
import { getMessaging } from '@platformatic/globals'
import type { PoolClient } from 'pg'
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
  validateBeginTransactionEnvelope,
  validateCancelRequest,
  validateLockRequestEnvelope,
  validateNonLockRequestEnvelope,
  validateQueryEnvelope,
} from './validation.js'

type MessageContext = { channel?: unknown } | undefined

type ClosableChannel = {
  once(event: 'close', listener: () => void): unknown
}

export class Application {
  #config: StorageConfigType
  #pools: PoolRegistry
  #locks: LockRegistry
  #cancellations: CancellationRegistry
  #shuttingDown: boolean
  #stats: Record<string, number>
  #trackedChannels = new WeakSet<object>()

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

  async #handleAcquire(rawRequest: unknown, context?: MessageContext): Promise<unknown> {
    this.#stats.acquire++
    let request: AcquireConnectionRequest | undefined

    try {
      validateNonLockRequestEnvelope(rawRequest)
      request = rawRequest as AcquireConnectionRequest

      this.#assertAcceptingWork()
      const client = await this.#pools.acquire(request.destination)
      const owner = this.#trackOwner(context)
      return { lockId: this.#locks.create(request.destination, client, false, owner) }
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
      const response = toErrorResponse(error, context ?? undefined)
      if (request && !this.#locks.has(request.lockId)) {
        // The lock was dropped (broken connection or expiry): tell the caller its
        // transaction is gone so it does not try to commit or roll it back.
        response.connectionDiscarded = true
      }
      return response
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

  async #handleBeginTransaction(rawRequest: unknown, context?: MessageContext): Promise<unknown> {
    this.#stats.beginTransaction++
    let request: BeginTransactionRequest | undefined

    try {
      validateBeginTransactionEnvelope(rawRequest)
      request = rawRequest as BeginTransactionRequest

      this.#assertAcceptingWork()
      const client = await this.#beginWithRetry(request)
      const owner = this.#trackOwner(context)
      return { lockId: this.#locks.create(request.destination, client, true, owner) }
    } catch (error) {
      return toErrorResponse(error, request ? this.#withDestinationContext(request) : undefined)
    }
  }

  // Statement timeout and search_path are set by the storage worker inside the
  // transaction (folded into its scope statement), so BEGIN is the only setup here.
  // Like PgTenantConnection, a broken pooled connection is disposed and retried.
  async #beginWithRetry(request: BeginTransactionRequest): Promise<PoolClient> {
    const beginStatement = buildBeginStatement({
      isolation: request.isolationLevel,
      readOnly: request.readOnly,
    })
    const startedAt = performance.now()
    let delayMs = TRANSACTION_SETUP_RETRY.minDelayMs

    for (let attempt = 1; ; attempt++) {
      const client = await this.#pools.acquire(request.destination)

      try {
        await runQuery(client, beginStatement)
        return client
      } catch (error) {
        client.release(error instanceof Error ? error : new Error(String(error)))

        const { sleepMs, nextDelayMs } = nextTransactionSetupRetryDelay(delayMs)
        if (
          !isRetryableTransactionSetupError(error) ||
          attempt >= TRANSACTION_SETUP_RETRY.maxAttempts ||
          performance.now() - startedAt + sleepMs >= TRANSACTION_SETUP_RETRY.totalBudgetMs
        ) {
          throw error
        }

        await new Promise((resolve) => setTimeout(resolve, sleepMs))
        delayMs = nextDelayMs
        this.#assertAcceptingWork()
      }
    }
  }

  // Locks are owned by the messaging channel of the storage worker that created
  // them. When that worker exits, the channel closes and its open transactions are
  // rolled back right away instead of holding row locks until the idle timeout.
  #trackOwner(context: MessageContext): object | undefined {
    const channel = context?.channel
    if (!isClosableChannel(channel)) {
      return undefined
    }

    if (!this.#trackedChannels.has(channel)) {
      this.#trackedChannels.add(channel)
      channel.once('close', () => {
        void this.#locks.releaseOwner(channel)
      })
    }

    return channel
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

      // An unknown lock was already discarded (broken connection, expiry, or a
      // restarted Database application): its transaction is gone, which is what a
      // rollback asks for.
      if (!this.#locks.has(request.lockId)) {
        return { rolledBack: true }
      }

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

function isClosableChannel(channel: unknown): channel is ClosableChannel & object {
  return (
    typeof channel === 'object' &&
    channel !== null &&
    typeof (channel as { once?: unknown }).once === 'function'
  )
}
