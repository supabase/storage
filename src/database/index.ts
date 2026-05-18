import { readConfig } from './config.js'
import { CancellationRegistry } from './cancellation.js'
import { DestinationResolver } from './destinations.js'
import { DatabaseWattError, toErrorResponse } from './errors.js'
import { LockRegistry } from './locks.js'
import { registerDatabaseWattMetrics } from './metrics.js'
import { PoolRegistry, runQuery } from './pools.js'
import { enforceResultLimits } from './result-limits.js'
import { startTestServer, type DatabaseWattTestServer } from './test-server.js'
import type {
  AcquireConnectionRequest,
  BeginTransactionRequest,
  CancelRequest,
  CommitTransactionRequest,
  LockedQueryRequest,
  QueryRequest,
  ReleaseConnectionRequest,
  RollbackTransactionRequest,
} from './types.js'
import {
  validateCancelRequest,
  validateLockRequestEnvelope,
  validateNonLockRequestEnvelope,
  validateQueryEnvelope,
} from './validation.js'

type MessagingApi = {
  handle: (message: string, handler: (data: unknown) => Promise<unknown> | unknown) => void
}

type PlatformaticGlobal = {
  messaging?: MessagingApi
}

export const hasServer = false

const config = readConfig()
const resolver = new DestinationResolver(config)
const pools = new PoolRegistry(config)
const locks = new LockRegistry(config)
const cancellations = new CancellationRegistry()
let shuttingDown = false
let testServer: DatabaseWattTestServer | undefined

const stats = {
  acquire: 0,
  beginTransaction: 0,
  cancel: 0,
  commitTransaction: 0,
  lockedQuery: 0,
  query: 0,
  release: 0,
  rollbackTransaction: 0,
}

registerDatabaseWattMetrics(pools)
registerHandlers()

export async function close(): Promise<void> {
  shuttingDown = true

  await withShutdownTimeout(
    Promise.allSettled([testServer?.close(), locks.close(), pools.close(), resolver.close()]).then(
      () => undefined
    ),
    config.shutdownTimeoutMs
  )
}

async function handleQuery(rawRequest: unknown): Promise<unknown> {
  stats.query++
  validateNonLockRequestEnvelope(rawRequest, config)
  validateQueryEnvelope(rawRequest, config)
  const request = rawRequest as QueryRequest

  return runHandler(request, async () => {
    assertAcceptingWork()
    const destination = await resolver.resolve(request.destination)
    cancellations.start(request.requestId, { cancelled: false })

    try {
      const response = await pools.query(
        destination,
        request.sql,
        request.values,
        (client) => cancellations.setClient(request.requestId, client)
      )
      return enforceResultLimits(response, config)
    } finally {
      cancellations.finish(request.requestId)
    }
  })
}

async function handleAcquire(rawRequest: unknown): Promise<unknown> {
  stats.acquire++
  validateNonLockRequestEnvelope(rawRequest, config)
  const request = rawRequest as AcquireConnectionRequest

  return runHandler(request, async () => {
    assertAcceptingWork()
    const destination = await resolver.resolve(request.destination)
    const client = await pools.acquire(destination)
    return { lockId: locks.create(destination, client) }
  })
}

async function handleLockedQuery(rawRequest: unknown): Promise<unknown> {
  stats.lockedQuery++
  validateLockRequestEnvelope(rawRequest, config)
  validateQueryEnvelope(rawRequest, config)
  const request = rawRequest as LockedQueryRequest

  return runHandler(withLockContext(request), async () => {
    assertAcceptingWork()
    cancellations.start(request.requestId, { cancelled: false, lockId: request.lockId })

    try {
      cancellations.setClient(request.requestId, locks.getClient(request.lockId))
      const response = await locks.query(request.lockId, request.sql, request.values)
      return enforceResultLimits(response, config)
    } finally {
      cancellations.finish(request.requestId)
    }
  })
}

async function handleRelease(rawRequest: unknown): Promise<unknown> {
  stats.release++
  validateLockRequestEnvelope(rawRequest, config)
  const request = rawRequest as ReleaseConnectionRequest

  return runHandler(withLockContext(request), async () => {
    await locks.release(request.lockId)
    return { released: true }
  })
}

async function handleBeginTransaction(rawRequest: unknown): Promise<unknown> {
  stats.beginTransaction++
  validateNonLockRequestEnvelope(rawRequest, config)
  const request = rawRequest as BeginTransactionRequest

  return runHandler(request, async () => {
    assertAcceptingWork()
    const destination = await resolver.resolve(request.destination)
    const client = await pools.acquire(destination)
    let lockId: string | undefined

    try {
      await runQuery(client, buildBeginStatement(request))
      if (config.serverStatementTimeoutMs > 0) {
        await runQuery(client, `SELECT set_config('statement_timeout', $1, true)`, [
          `${config.serverStatementTimeoutMs}ms`,
        ])
      }
      lockId = locks.create(destination, client, true)
      return { lockId }
    } catch (error) {
      client.release(error instanceof Error ? error : new Error(String(error)))
      throw error
    }
  })
}

async function handleCommitTransaction(rawRequest: unknown): Promise<unknown> {
  stats.commitTransaction++
  validateLockRequestEnvelope(rawRequest, config)
  const request = rawRequest as CommitTransactionRequest

  return runHandler(withLockContext(request), async () => {
    await locks.commit(request.lockId)
    return { committed: true }
  })
}

async function handleRollbackTransaction(rawRequest: unknown): Promise<unknown> {
  stats.rollbackTransaction++
  validateLockRequestEnvelope(rawRequest, config)
  const request = rawRequest as RollbackTransactionRequest

  return runHandler(withLockContext(request), async () => {
    await locks.rollback(request.lockId)
    return { rolledBack: true }
  })
}

async function handleCancel(rawRequest: unknown): Promise<unknown> {
  stats.cancel++
  validateCancelRequest(rawRequest)
  const request = rawRequest as CancelRequest

  return runHandler({ requestId: request.requestId, lockId: request.lockId }, async () => {
    return cancellations.cancel(request.requestId, request.lockId)
  })
}

function registerHandlers(): void {
  const platformatic = (globalThis as typeof globalThis & { platformatic?: PlatformaticGlobal })
    .platformatic
  const messaging = platformatic?.messaging

  if (!messaging) {
    return
  }

  messaging.handle('database.query', wrapHandler(handleQuery))
  messaging.handle('database.acquire', wrapHandler(handleAcquire))
  messaging.handle('database.lockedQuery', wrapHandler(handleLockedQuery))
  messaging.handle('database.release', wrapHandler(handleRelease))
  messaging.handle('database.beginTransaction', wrapHandler(handleBeginTransaction))
  messaging.handle('database.commitTransaction', wrapHandler(handleCommitTransaction))
  messaging.handle('database.rollbackTransaction', wrapHandler(handleRollbackTransaction))
  messaging.handle('database.cancel', wrapHandler(handleCancel))

}

function resetStats(): void {
  for (const key of Object.keys(stats) as Array<keyof typeof stats>) {
    stats[key] = 0
  }
}

function startDatabaseTestServer(): void {
  if (process.env.DATABASE_WATT_TEST_SERVER !== 'true') {
    return
  }

  startTestServer({ handlers: databaseApp, resetStats, stats })
    .then((server) => {
      testServer = server
    })
    .catch((error) => {
      process.nextTick(() => {
        throw error
      })
    })
}

function wrapHandler(
  handler: (data: unknown) => Promise<unknown>
): (data: unknown) => Promise<unknown> {
  return async (data) => {
    try {
      return await handler(data)
    } catch (error) {
      return toErrorResponse(error)
    }
  }
}

async function runHandler(
  context: {
    destination?: string
    lockId?: string
    operationName?: string
    requestId?: string
  },
  fn: () => Promise<unknown>
): Promise<unknown> {
  try {
    return await fn()
  } catch (error) {
    return toErrorResponse(error, context)
  }
}

function withLockContext<T extends { lockId: string; operationName?: string; requestId?: string }>(
  request: T
): T & { destination?: string } {
  return {
    ...request,
    destination: locks.getDestination(request.lockId),
  }
}

function assertAcceptingWork(): void {
  if (shuttingDown) {
    throw new DatabaseWattError('SHUTDOWN', 'Database application is shutting down')
  }
}

function buildBeginStatement(request: BeginTransactionRequest): string {
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

function normalizeIsolationLevel(isolationLevel: string | undefined): string | undefined {
  switch (isolationLevel) {
    case 'read committed':
      return 'READ COMMITTED'
    case 'repeatable read':
      return 'REPEATABLE READ'
    case 'serializable':
      return 'SERIALIZABLE'
    default:
      return undefined
  }
}

async function withShutdownTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
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

export const databaseApp = {
  close,
  handleAcquire,
  handleBeginTransaction,
  handleCancel,
  handleCommitTransaction,
  handleLockedQuery,
  handleQuery,
  handleRelease,
  handleRollbackTransaction,
}

startDatabaseTestServer()
