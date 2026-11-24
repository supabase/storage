import {
  isConnectionStateError,
  isRetryableTransactionSetupError,
} from '@internal/database/postgres/pool-errors'
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
import { BeginError, PoolRegistry, runBatch, toBatchResponse } from './pools.js'
import {
  type AcquireConnectionRequest,
  type BatchRequest,
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
  validateBatchEnvelope,
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
      batch: 0,
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

    // The handlers return plain data, with failures as error responses rather than thrown
    // errors, so their results can be posted without Platformatic walking them first.
    const plainData = { sanitize: false }
    messaging.handle(DATABASE_MESSAGES.query, this.#handleQuery.bind(this), plainData)
    messaging.handle(DATABASE_MESSAGES.acquire, this.#handleAcquire.bind(this), plainData)
    messaging.handle(DATABASE_MESSAGES.batch, this.#handleBatch.bind(this), plainData)
    messaging.handle(DATABASE_MESSAGES.lockedQuery, this.#handleLockedQuery.bind(this), plainData)
    messaging.handle(DATABASE_MESSAGES.release, this.#handleRelease.bind(this), plainData)
    messaging.handle(
      DATABASE_MESSAGES.beginTransaction,
      this.#handleBeginTransaction.bind(this),
      plainData
    )
    messaging.handle(
      DATABASE_MESSAGES.commitTransaction,
      this.#handleCommitTransaction.bind(this),
      plainData
    )
    messaging.handle(
      DATABASE_MESSAGES.rollbackTransaction,
      this.#handleRollbackTransaction.bind(this),
      plainData
    )
    messaging.handle(DATABASE_MESSAGES.cancel, this.#handleCancel.bind(this), plainData)
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
      const owner = this.#trackOwner(context)
      const client = await this.#pools.acquire(request.destination)
      return { lockId: this.#locks.create(request.destination, client, false, owner) }
    } catch (error) {
      return toErrorResponse(error, request ? this.#withDestinationContext(request) : undefined)
    }
  }

  async #handleLockedQuery(rawRequest: unknown, context?: MessageContext): Promise<unknown> {
    this.#stats.lockedQuery++
    let request: LockedQueryRequest | undefined
    let destination: string | undefined
    let cancellationRequestId: string | undefined

    try {
      validateLockRequestEnvelope(rawRequest)
      validateQueryEnvelope(rawRequest)
      request = rawRequest as LockedQueryRequest
      destination = this.#locks.getDestination(request.lockId)

      this.#assertAcceptingWork()
      this.#cancellations.start(request.requestId, { cancelled: false, lockId: request.lockId })
      cancellationRequestId = request.requestId

      const owner = getOwner(context)
      this.#cancellations.setClient(request.requestId, this.#locks.getClient(request.lockId, owner))
      const response = await this.#locks.query(request.lockId, request.sql, request.values, owner)
      return response
    } catch (error) {
      const response = toErrorResponse(error, request ? { ...request, destination } : undefined)
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

  async #handleRelease(rawRequest: unknown, context?: MessageContext): Promise<unknown> {
    this.#stats.release++
    let request: ReleaseConnectionRequest | undefined
    let destination: string | undefined

    try {
      validateLockRequestEnvelope(rawRequest)
      request = rawRequest as ReleaseConnectionRequest
      destination = this.#locks.getDestination(request.lockId)

      await this.#locks.release(request.lockId, getOwner(context))
      return { released: true }
    } catch (error) {
      return toErrorResponse(error, request ? { ...request, destination } : undefined)
    }
  }

  async #handleBeginTransaction(rawRequest: unknown, context?: MessageContext): Promise<unknown> {
    this.#stats.beginTransaction++
    let request: BeginTransactionRequest | undefined

    try {
      validateBeginTransactionEnvelope(rawRequest)
      request = rawRequest as BeginTransactionRequest

      this.#assertAcceptingWork()
      const owner = this.#trackOwner(context)
      const client = await this.#beginWithRetry(request)
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
        await client.query(beginStatement)
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
  // Called before a connection is taken, so a request without a channel gets none.
  #trackOwner(context: MessageContext): object {
    const channel = context?.channel
    if (!isClosableChannel(channel)) {
      throw new DatabaseWattError('PROTOCOL_ERROR', 'A lock can only be created over a channel')
    }

    if (!this.#trackedChannels.has(channel)) {
      this.#trackedChannels.add(channel)
      channel.once('close', () => {
        void this.#locks.releaseOwner(channel)
      })
    }

    return channel
  }

  async #handleCommitTransaction(rawRequest: unknown, context?: MessageContext): Promise<unknown> {
    this.#stats.commitTransaction++
    let request: CommitTransactionRequest | undefined
    let destination: string | undefined

    try {
      validateLockRequestEnvelope(rawRequest)
      request = rawRequest as CommitTransactionRequest
      destination = this.#locks.getDestination(request.lockId)

      await this.#locks.commit(request.lockId, getOwner(context))
      return { committed: true }
    } catch (error) {
      return toErrorResponse(error, request ? { ...request, destination } : undefined)
    }
  }

  async #handleRollbackTransaction(
    rawRequest: unknown,
    context?: MessageContext
  ): Promise<unknown> {
    this.#stats.rollbackTransaction++
    let request: RollbackTransactionRequest | undefined
    let destination: string | undefined

    try {
      validateLockRequestEnvelope(rawRequest)
      request = rawRequest as RollbackTransactionRequest
      destination = this.#locks.getDestination(request.lockId)

      // An unknown lock was already discarded (broken connection, expiry, or a
      // restarted Database application): its transaction is gone, which is what a
      // rollback asks for.
      if (!this.#locks.has(request.lockId)) {
        return { rolledBack: true }
      }

      await this.#locks.rollback(request.lockId, getOwner(context))
      return { rolledBack: true }
    } catch (error) {
      return toErrorResponse(error, request ? { ...request, destination } : undefined)
    }
  }

  // BEGIN, the statements and (with commit) COMMIT for one message. A transaction left
  // open becomes a lock owned by the caller's channel. On failure the transaction is
  // rolled back and the response says so (connectionDiscarded), so the caller does not
  // send a rollback of its own.
  async #handleBatch(rawRequest: unknown, context?: MessageContext): Promise<unknown> {
    this.#stats.batch++
    let request: BatchRequest | undefined
    let cancellationRequestId: string | undefined

    try {
      validateBatchEnvelope(rawRequest)
      request = rawRequest as BatchRequest

      this.#assertAcceptingWork()
      // A transaction left open is owned by the caller's channel.
      const owner = request.commit ? undefined : this.#trackOwner(context)
      this.#cancellations.start(request.requestId, { cancelled: false })
      cancellationRequestId = request.requestId

      const { client, results } = await this.#runBatchWithRetry(request)
      const response = toBatchResponse(request.statements, results)

      if (!owner) {
        client.release()
        return response
      }

      response.lockId = this.#locks.create(request.destination, client, true, owner)
      return response
    } catch (error) {
      const response = toErrorResponse(
        error instanceof BeginError ? error.original : error,
        request ? this.#withDestinationContext(request) : undefined
      )
      // A failed batch never leaves a transaction behind.
      response.connectionDiscarded = true
      return response
    } finally {
      this.#cancellations.finish(cancellationRequestId)
    }
  }

  async #runBatchWithRetry(request: BatchRequest) {
    const beginStatement = buildBeginStatement({
      isolation: request.isolationLevel,
      readOnly: request.readOnly,
    })
    const startedAt = performance.now()
    let delayMs = TRANSACTION_SETUP_RETRY.minDelayMs

    for (let attempt = 1; ; attempt++) {
      const client = await this.#pools.acquire(request.destination)
      this.#cancellations.setClient(request.requestId, client)

      try {
        const results = await runBatch(client, beginStatement, request.statements, request.commit)
        return { client, results }
      } catch (error) {
        if (!(error instanceof BeginError)) {
          await rollbackAndRelease(client, error)
          throw error
        }

        // BEGIN failed, so nothing ran: like #beginWithRetry, retry on another connection.
        client.release(toError(error.original))

        const { sleepMs, nextDelayMs } = nextTransactionSetupRetryDelay(delayMs)
        if (
          !isRetryableTransactionSetupError(error.original) ||
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

  #withDestinationContext(
    request: QueryRequest | AcquireConnectionRequest | BatchRequest
  ): ErrorContext {
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

function getOwner(context: MessageContext): object | undefined {
  const channel = context?.channel
  return isClosableChannel(channel) ? channel : undefined
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}

// After a failed batch the transaction may still be open (aborted). ROLLBACK closes it;
// when COMMIT already ended it, PostgreSQL only warns that no transaction is in progress.
async function rollbackAndRelease(client: PoolClient, error: unknown) {
  let releaseError = isConnectionStateError(error) ? toError(error) : undefined

  if (!releaseError) {
    try {
      await client.query('ROLLBACK')
    } catch (rollbackError) {
      releaseError = toError(rollbackError)
    }
  }

  client.release(releaseError)
}

function isClosableChannel(channel: unknown): channel is ClosableChannel & object {
  return (
    typeof channel === 'object' &&
    channel !== null &&
    typeof (channel as { once?: unknown }).once === 'function'
  )
}
