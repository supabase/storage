import { randomUUID } from 'node:crypto'
import fastify, { type FastifyInstance, type FastifyReply } from 'fastify'

type DatabaseWattStats = {
  acquire: number
  beginTransaction: number
  cancel: number
  commitTransaction: number
  lockedQuery: number
  query: number
  release: number
  rollbackTransaction: number
}

type DatabaseWattHandlers = {
  handleBeginTransaction(data: unknown): Promise<unknown>
  handleCancel(data: unknown): Promise<unknown>
  handleCommitTransaction(data: unknown): Promise<unknown>
  handleLockedQuery(data: unknown): Promise<unknown>
  handleQuery(data: unknown): Promise<unknown>
  handleRollbackTransaction(data: unknown): Promise<unknown>
}

type DatabaseWattTestServerOptions = {
  handlers: DatabaseWattHandlers
  resetStats(): void
  stats: DatabaseWattStats
}

type LockResponse = {
  lockId: string
}

export async function startTestServer({ handlers, resetStats, stats }: DatabaseWattTestServerOptions) {
  const port = Number.parseInt(process.env.DATABASE_WATT_TEST_PORT || '5001', 10)
  const server = fastify({ logger: false })

  server.get('/stats', async () => ({ ...stats }))
  server.post('/reset', async () => {
    resetStats()
    return { reset: true }
  })

  server.post('/query', async (_request, reply) => {
    return sendDatabaseResult(
      reply,
      await handlers.handleQuery({
        destination: testDestination(),
        requestId: randomUUID(),
        sql: 'SELECT 1 as value',
      })
    )
  })

  server.post('/master-query', async (_request, reply) => {
    return sendDatabaseResult(
      reply,
      await handlers.handleQuery({
        destination: 'master',
        requestId: randomUUID(),
        sql: 'SELECT 1 as value',
      })
    )
  })

  server.post('/master-transaction', async () => {
    const tx = await beginTransaction(handlers, 'master')

    try {
      const result = await checkedLockedQuery<{ rows: Array<{ value: number }> }>(handlers, tx.lockId, {
        text: 'SELECT 1 as value',
      })
      await checkedResult(handlers.handleCommitTransaction({ lockId: tx.lockId }))
      return { value: result.rows[0]?.value }
    } catch (error) {
      await handlers.handleRollbackTransaction({ lockId: tx.lockId }).catch(() => undefined)
      throw error
    }
  })

  server.post('/missing-destination', async (_request, reply) => {
    return sendDatabaseResult(
      reply,
      await handlers.handleQuery({
        destination: `missing-${randomUUID()}`,
        requestId: randomUUID(),
        sql: 'SELECT 1',
      })
    )
  })

  server.post('/concurrent-queries', async (_request, reply) => {
    const results = await Promise.all(
      Array.from({ length: 5 }, () =>
        handlers.handleQuery({
          destination: testDestination(),
          requestId: randomUUID(),
          sql: 'SELECT 1 as value',
        })
      )
    )

    for (const result of results) {
      if (isDatabaseError(result)) {
        return sendDatabaseResult(reply, result)
      }
    }

    return { count: results.length }
  })

  server.post('/sleep', async (_request, reply) => {
    const requestId = randomUUID()
    const query = handlers.handleQuery({
      destination: testDestination(),
      requestId,
      sql: 'SELECT pg_sleep(10)',
    })

    setTimeout(() => {
      handlers.handleCancel({ requestId }).catch(() => undefined)
    }, 50).unref()

    return sendDatabaseResult(reply, await query)
  })

  server.post('/rollback', async () => {
    const bucketName = `db-watt-rollback-${Date.now()}`
    const tx = await beginTransaction(handlers)

    try {
      await checkedLockedQuery(handlers, tx.lockId, {
        text: `INSERT INTO storage.buckets (id, name, owner, public) VALUES ($1, $1, $2, false)`,
        values: [bucketName, randomUUID()],
      })
      await checkedResult(handlers.handleRollbackTransaction({ lockId: tx.lockId }))
      return { bucketName }
    } catch (error) {
      await handlers.handleRollbackTransaction({ lockId: tx.lockId }).catch(() => undefined)
      throw error
    }
  })

  server.post('/savepoint', async () => {
    const innerBucket = `db-watt-savepoint-inner-${Date.now()}`
    const outerBucket = `db-watt-savepoint-outer-${Date.now()}`
    const tx = await beginTransaction(handlers)

    try {
      await checkedLockedQuery(handlers, tx.lockId, {
        text: `INSERT INTO storage.buckets (id, name, owner, public) VALUES ($1, $1, $2, false)`,
        values: [outerBucket, randomUUID()],
      })
      await checkedLockedQuery(handlers, tx.lockId, { text: 'SAVEPOINT database_watt_acceptance' })
      await checkedLockedQuery(handlers, tx.lockId, {
        text: `INSERT INTO storage.buckets (id, name, owner, public) VALUES ($1, $1, $2, false)`,
        values: [innerBucket, randomUUID()],
      })
      await checkedLockedQuery(handlers, tx.lockId, {
        text: 'ROLLBACK TO SAVEPOINT database_watt_acceptance',
      })
      await checkedResult(handlers.handleCommitTransaction({ lockId: tx.lockId }))
      return { innerBucket, outerBucket }
    } catch (error) {
      await handlers.handleRollbackTransaction({ lockId: tx.lockId }).catch(() => undefined)
      throw error
    }
  })

  await server.listen({ host: '127.0.0.1', port })
  return server
}

async function beginTransaction(
  handlers: DatabaseWattHandlers,
  destination = testDestination()
): Promise<LockResponse> {
  return checkedResult(
    handlers.handleBeginTransaction({
      destination,
      requestId: randomUUID(),
    })
  )
}

async function checkedLockedQuery<T = unknown>(
  handlers: DatabaseWattHandlers,
  lockId: string,
  query: { text: string; values?: unknown[] }
): Promise<T> {
  return checkedResult(
    handlers.handleLockedQuery({
      lockId,
      requestId: randomUUID(),
      sql: query.text,
      values: query.values,
    })
  )
}

async function checkedResult<T = unknown>(resultPromise: Promise<unknown>): Promise<T> {
  const result = await resultPromise
  if (isDatabaseError(result)) {
    const error = result as { message: string }
    throw new Error(error.message)
  }
  return result as T
}

function sendDatabaseResult(reply: FastifyReply, result: unknown): unknown {
  if (isDatabaseError(result)) {
    return reply.status(500).send(result)
  }

  return result
}

function isDatabaseError(result: unknown): boolean {
  return typeof result === 'object' && result !== null && 'code' in result && 'message' in result
}

function testDestination(): string {
  return process.env.TENANT_ID || 'default'
}

export type DatabaseWattTestServer = FastifyInstance
