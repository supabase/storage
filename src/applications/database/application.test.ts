import { getMessaging } from '@platformatic/globals'
import { setupLoopbackMessaging } from '@platformatic/runtime'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { DatabaseErrorResponse } from './errors.js'
import type { create } from './index.js'
import { DATABASE_MESSAGES } from './protocol.js'

type Handler = (data: unknown, context?: unknown) => Promise<unknown>
type HandlerOptions = { sanitize?: boolean } | undefined

type MockClient = {
  queries: Array<{ sql: string; values?: unknown[] }>
  query: ReturnType<typeof vi.fn>
  release: ReturnType<typeof vi.fn>
}

type MockedEnvironment = {
  app: ReturnType<typeof create>
  messaging: ReturnType<typeof setupLoopbackMessaging>
  handlers: Map<string, Handler>
  handlerOptions: Map<string, HandlerOptions>
  clients: MockClient[]
  pools: Array<{
    config: Record<string, unknown>
    options: Record<string, unknown>
    ended: boolean
    idleCount: number
    totalCount: number
    queries: Array<{ sql: string; values?: unknown[] }>
  }>
}

type QueryConfig = { text: string; values?: unknown[]; rowMode?: string }

function createMockClient(
  rows: Record<string, unknown>[],
  failures: { begin: number; sql?: string }
): MockClient {
  const client: MockClient = {
    queries: [],
    query: vi.fn(async (config: string | QueryConfig, values?: unknown[]) => {
      const sql = typeof config === 'string' ? config : config.text
      client.queries.push({ sql, values: typeof config === 'string' ? values : config.values })
      if (sql.startsWith('BEGIN') && failures.begin > 0) {
        failures.begin--
        throw new Error('Connection terminated unexpectedly')
      }
      if (sql === failures.sql) {
        throw new Error('relation "missing" does not exist')
      }
      // Statements run for storage workers use array row mode with raw text values.
      if (typeof config !== 'string' && config.rowMode === 'array') {
        return toRawResult(rows)
      }
      return { rowCount: rows.length, rows, fields: [] }
    }),
    release: vi.fn(),
  }

  return client
}

function toRawResult(rows: Record<string, unknown>[]) {
  const names = rows.length > 0 ? Object.keys(rows[0]) : []
  return {
    rowCount: rows.length,
    rows: rows.map((row) => names.map((name) => (row[name] === null ? null : String(row[name])))),
    fields: names.map((name) => ({ name, dataTypeID: 25 })),
  }
}

// The response for the default `[{ ok: true }]` rows.
const okResponse = { rowCount: 1, rows: [['true']], fields: [{ name: 'ok', dataTypeID: 25 }] }

async function loadApp(
  options: {
    env?: Record<string, string>
    queryRows?: Record<string, unknown>[]
    beginFailures?: number
    failingSql?: string
  } = {}
): Promise<MockedEnvironment> {
  vi.resetModules()

  const clients: MockClient[] = []
  const pools: MockedEnvironment['pools'] = []
  const queryRows = options.queryRows || [{ ok: true }]
  const failures = { begin: options.beginFailures ?? 0, sql: options.failingSql }

  process.env = {
    ...originalEnv,
    AUTH_JWT_SECRET: 'test-secret',
    DATABASE_URL: 'postgres://single-tenant',
    MULTI_TENANT: 'false',
    ...options.env,
  }

  vi.doMock('pg', () => {
    const types = {
      getTypeParser: vi.fn(),
      setTypeParser: vi.fn(),
    }

    class MockDatabaseError extends Error {
      code?: string
    }

    class MockPool {
      totalCount = 0
      idleCount = 0
      waitingCount = 0
      ended = false
      on = vi.fn()
      queries: Array<{ sql: string; values?: unknown[] }> = []
      config: Record<string, unknown>
      options: Record<string, unknown>

      constructor(config: Record<string, unknown>) {
        this.config = config
        this.options = { ...config }
        pools.push(this)
      }

      async connect() {
        this.totalCount++
        const client = createMockClient(queryRows, failures)
        clients.push(client)
        return client
      }

      async end() {
        this.ended = true
      }
    }

    return {
      DatabaseError: MockDatabaseError,
      Pool: MockPool,
      types,
      default: { types },
    }
  })

  vi.doMock('pg/lib/connection', () => ({
    default: class MockPgConnection {},
  }))

  // We need dynamic import due to the mocking of PostgreSQL modules above
  const { create } = await import('./index.js')

  const messaging = setupLoopbackMessaging('db')
  const { handlers, handlerOptions } = captureHandlers()
  const app = create()
  return { app, messaging, handlers, handlerOptions, clients, pools }
}

// Records the handlers the application registers and their options, to call one without a
// messaging channel.
function captureHandlers() {
  const handlers = new Map<string, Handler>()
  const handlerOptions = new Map<string, HandlerOptions>()
  const messaging = getMessaging() as {
    handle(name: string, handler: Handler, options?: HandlerOptions): void
  }
  const handle = messaging.handle
  messaging.handle = (name, handler, options) => {
    handlers.set(name, handler)
    handlerOptions.set(name, options)
    handle(name, handler, options)
  }
  return { handlers, handlerOptions }
}

const originalEnv = { ...process.env }

afterEach(async () => {
  vi.doUnmock('pg')
  vi.doUnmock('pg/lib/connection')
  vi.resetModules()
  process.env = { ...originalEnv }
})

describe('database Watt application messaging handlers', () => {
  it('registers prefixed handlers and exposes no server', async () => {
    const { app } = await loadApp()

    expect(app.isBackgroundApplication).toBe(true)
  })

  it('posts the results of the database handlers without sanitizing them', async () => {
    const { handlerOptions } = await loadApp()

    for (const message of Object.values(DATABASE_MESSAGES)) {
      expect(handlerOptions.get(message), message).toEqual({ sanitize: false })
    }
  })

  it('executes stateless queries against the single-tenant destination', async () => {
    const { messaging, clients } = await loadApp()

    const response = await messaging.send('foo', 'database.query', {
      destination: createDestination(),
      requestId: 'req-1',
      sql: 'SELECT 1',
      values: [1],
    })

    expect(response).toEqual(okResponse)
    expect(clients[0].queries).toEqual([{ sql: 'SELECT 1', values: [1] }])
    expect(clients[0].release).toHaveBeenCalledWith(undefined)
  })

  it('uses caller-resolved external pool settings', async () => {
    const { messaging, pools } = await loadApp()

    await messaging.send('database', 'database.query', {
      destination: createDestination({
        connectionString: 'postgres://pooler',
        isExternalPool: true,
      }),
      sql: 'SELECT 1',
    })

    expect(pools[0].config).toMatchObject({ connectionString: 'postgres://pooler' })
  })

  it('returns validation errors from malformed requests', async () => {
    const { messaging } = await loadApp()

    const response = (await messaging.send('database', 'database.query', {
      destination: createDestination({ id: '' }),
      sql: 'SELECT 1',
    })) as DatabaseErrorResponse

    expect(response).toMatchObject({
      code: 'PROTOCOL_ERROR',
      message: 'destination.id must be a non-empty string',
    })
  })

  it('acquires and releases pinned connections', async () => {
    const { clients, messaging } = await loadApp()

    const acquire = (await messaging.send('database', 'database.acquire', {
      destination: createDestination(),
    })) as { lockId: string }

    expect(acquire.lockId).toBeTruthy()

    const release = await messaging.send('database', 'database.release', { lockId: acquire.lockId })

    expect(release).toEqual({ released: true })
    expect(clients[0].release).toHaveBeenCalledWith(undefined)
  })

  it('creates no lock and takes no connection for a request without a channel', async () => {
    const { clients, handlers } = await loadApp()
    const destination = createDestination()
    const statements = [{ sql: 'SELECT 1' }]

    for (const [message, request] of [
      ['database.acquire', { destination }],
      ['database.beginTransaction', { destination }],
      ['database.batch', { destination, statements, commit: false }],
    ] as const) {
      await expect(handlers.get(message)?.(request, {})).resolves.toMatchObject({
        code: 'PROTOCOL_ERROR',
        message: 'A lock can only be created over a channel',
      })
    }
    expect(clients).toHaveLength(0)

    // A committed batch leaves no lock behind.
    await expect(
      handlers.get('database.batch')?.({ destination, statements, commit: true }, {})
    ).resolves.toEqual(okResponse)
  })

  it('only serves a lock to the channel that created it', async () => {
    const { clients, handlers, messaging } = await loadApp()

    const { lockId } = (await messaging.send('database', 'database.beginTransaction', {
      destination: createDestination(),
    })) as { lockId: string }

    for (const context of [{}, { channel: { once() {} } }]) {
      await expect(
        handlers.get('database.lockedQuery')?.({ lockId, sql: 'SELECT 1' }, context)
      ).resolves.toMatchObject({ code: 'PROTOCOL_ERROR', message: 'Unknown lock ID' })
      await expect(
        handlers.get('database.commitTransaction')?.({ lockId }, context)
      ).resolves.toMatchObject({ code: 'PROTOCOL_ERROR', message: 'Unknown lock ID' })
    }

    await expect(
      messaging.send('database', 'database.commitTransaction', { lockId })
    ).resolves.toEqual({ committed: true })
    expect(clients[0].queries.map((query) => query.sql)).toEqual(['BEGIN', 'COMMIT'])
  })

  it('runs transaction lifecycle on one pinned connection', async () => {
    const { clients, messaging } = await loadApp()

    const begin = (await messaging.send('database', 'database.beginTransaction', {
      destination: createDestination(),
      isolationLevel: 'serializable',
      readOnly: true,
    })) as { lockId: string }

    expect(begin.lockId).toBeTruthy()

    await messaging.send('database', 'database.lockedQuery', {
      lockId: begin.lockId,
      sql: 'SELECT 1',
    })
    const commit = await messaging.send('database', 'database.commitTransaction', {
      lockId: begin.lockId,
    })

    expect(commit).toEqual({ committed: true })
    // statement_timeout/search_path are folded into the storage worker's scope statement.
    expect(clients[0].queries.map((query) => query.sql)).toEqual([
      'BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY',
      'SELECT 1',
      'COMMIT',
    ])
    expect(clients[0].release).toHaveBeenCalledWith(undefined)
  })

  it.each([
    [{ readOnly: 'false' }, 'readOnly must be a boolean when present'],
    [
      { isolationLevel: 'chaos' },
      'isolationLevel must be one of read committed, repeatable read, serializable',
    ],
  ])('rejects malformed transaction options %o', async (options, message) => {
    const { clients, messaging } = await loadApp()

    const response = (await messaging.send('database', 'database.beginTransaction', {
      destination: createDestination(),
      ...options,
    })) as DatabaseErrorResponse

    expect(response).toMatchObject({ code: 'PROTOCOL_ERROR', message })
    expect(clients).toHaveLength(0)
  })

  it('retries BEGIN on a broken pooled connection', async () => {
    const { clients, messaging } = await loadApp({ beginFailures: 1 })

    const begin = (await messaging.send('database', 'database.beginTransaction', {
      destination: createDestination(),
    })) as { lockId: string }

    expect(begin.lockId).toBeTruthy()
    expect(clients).toHaveLength(2)
    expect(clients[0].release).toHaveBeenCalledWith(expect.any(Error))
    expect(clients[1].queries.map((query) => query.sql)).toEqual(['BEGIN'])
  })

  it('treats a rollback of an unknown lock as already rolled back', async () => {
    const { messaging } = await loadApp()

    const response = await messaging.send('database', 'database.rollbackTransaction', {
      lockId: 'unknown-lock',
    })

    expect(response).toEqual({ rolledBack: true })
  })

  it('queues on a full destination pool instead of failing the global budget check', async () => {
    const { messaging } = await loadApp({
      env: { DATABASE_WATT_GLOBAL_MAX_CONNECTIONS: '1' },
    })
    const destination = createDestination({ maxConnections: 1 })

    // Holds the only connection the budget allows.
    await messaging.send('database', 'database.beginTransaction', { destination })
    const response = await messaging.send('database', 'database.query', {
      destination,
      sql: 'SELECT 1',
    })

    expect(response).toEqual(okResponse)
  })

  it('reclaims idle pools of other destinations when the global budget is exhausted', async () => {
    const { messaging, pools } = await loadApp({
      env: { DATABASE_WATT_GLOBAL_MAX_CONNECTIONS: '1' },
    })

    await messaging.send('database', 'database.query', {
      destination: createDestination({ id: 'tenant-a' }),
      sql: 'SELECT 1',
    })
    pools[0].idleCount = pools[0].totalCount

    const response = await messaging.send('database', 'database.query', {
      destination: createDestination({ id: 'tenant-b', connectionString: 'postgres://b' }),
      sql: 'SELECT 1',
    })

    expect(response).toEqual(okResponse)
    expect(pools[0].ended).toBe(true)
  })

  it('rejects new connections when the global budget is held by busy connections', async () => {
    const { messaging } = await loadApp({
      env: { DATABASE_WATT_GLOBAL_MAX_CONNECTIONS: '1' },
    })

    await messaging.send('database', 'database.beginTransaction', {
      destination: createDestination({ id: 'tenant-a' }),
    })
    const response = (await messaging.send('database', 'database.query', {
      destination: createDestination({ id: 'tenant-b', connectionString: 'postgres://b' }),
      sql: 'SELECT 1',
    })) as DatabaseErrorResponse

    expect(response).toMatchObject({
      code: 'BUSY',
      message: 'Global connection budget is exhausted',
    })
  })

  it('resizes a destination pool in place when only its capacity changes', async () => {
    const { messaging, pools } = await loadApp()

    await messaging.send('database', 'database.query', {
      destination: createDestination({ maxConnections: 10 }),
      sql: 'SELECT 1',
    })
    await messaging.send('database', 'database.query', {
      destination: createDestination({ maxConnections: 4 }),
      sql: 'SELECT 1',
    })

    expect(pools).toHaveLength(1)
    expect(pools[0].ended).toBe(false)
    expect(pools[0].options.max).toBe(4)
  })

  it('sets search_path on direct destinations only', async () => {
    const { messaging, pools } = await loadApp()

    await messaging.send('database', 'database.query', {
      destination: createDestination({ id: 'direct' }),
      sql: 'SELECT 1',
    })
    await messaging.send('database', 'database.query', {
      destination: createDestination({
        id: 'pooler',
        connectionString: 'postgres://pooler',
        isExternalPool: true,
      }),
      sql: 'SELECT 1',
    })

    expect(pools[0].config.options).toMatch(/^-c search_path=storage,public,extensions/)
    expect(pools[1].config.options).toBeUndefined()
  })

  it('replaces a physical pool when Storage sends changed connection details', async () => {
    const { messaging, pools } = await loadApp()

    await messaging.send('database', 'database.query', {
      destination: createDestination({ connectionString: 'postgres://tenant-db-a' }),
      sql: 'SELECT 1',
    })
    await messaging.send('database', 'database.query', {
      destination: createDestination({ connectionString: 'postgres://tenant-db-b' }),
      sql: 'SELECT 1',
    })

    expect(pools).toHaveLength(2)
    expect(pools[0].ended).toBe(true)
    expect(pools[1].config).toMatchObject({ connectionString: 'postgres://tenant-db-b' })
  })

  it('runs a whole single-statement transaction from one batch message', async () => {
    const { clients, messaging } = await loadApp()

    const response = await messaging.send('database', 'database.batch', {
      destination: createDestination(),
      requestId: 'req-1',
      statements: [
        { sql: 'SELECT set_config($1, $2, true)', values: ['role', 'authenticated'] },
        { sql: 'SELECT 1', values: [1] },
      ],
      commit: true,
    })

    expect(response).toEqual(okResponse)
    expect(clients).toHaveLength(1)
    expect(clients[0].queries).toEqual([
      { sql: 'BEGIN', values: undefined },
      { sql: 'SELECT set_config($1, $2, true)', values: ['role', 'authenticated'] },
      { sql: 'SELECT 1', values: [1] },
      { sql: 'COMMIT', values: undefined },
    ])
    expect(clients[0].release).toHaveBeenCalledTimes(1)
    expect(clients[0].release.mock.calls[0][0]).toBeUndefined()
  })

  it('returns the results of the batch statements flagged with result', async () => {
    const { clients, messaging } = await loadApp()

    const response = await messaging.send('database', 'database.batch', {
      destination: createDestination(),
      statements: [
        { sql: 'SELECT set_config($1, $2, true)', values: ['role', 'service_role'] },
        { sql: 'SELECT 1', result: true },
        { sql: 'SELECT set_config($1, $2, true)', values: ['role', 'authenticated'] },
        { sql: 'SELECT 2' },
      ],
      commit: true,
    })

    expect(response).toEqual({ ...okResponse, results: [okResponse] })
    expect(clients[0].queries.map((query) => query.sql)).toEqual([
      'BEGIN',
      'SELECT set_config($1, $2, true)',
      'SELECT 1',
      'SELECT set_config($1, $2, true)',
      'SELECT 2',
      'COMMIT',
    ])
  })

  it('opens a transaction from a batch message and keeps it pinned', async () => {
    const { clients, messaging } = await loadApp()

    const batch = (await messaging.send('database', 'database.batch', {
      destination: createDestination(),
      isolationLevel: 'serializable',
      statements: [{ sql: 'SELECT 1' }],
      commit: false,
    })) as typeof okResponse & { lockId: string }

    expect(batch).toMatchObject(okResponse)
    expect(batch.lockId).toEqual(expect.any(String))
    expect(clients[0].release).not.toHaveBeenCalled()

    await messaging.send('database', 'database.lockedQuery', {
      lockId: batch.lockId,
      sql: 'SELECT 2',
    })
    const commit = await messaging.send('database', 'database.commitTransaction', {
      lockId: batch.lockId,
    })

    expect(commit).toEqual({ committed: true })
    expect(clients[0].queries.map((query) => query.sql)).toEqual([
      'BEGIN ISOLATION LEVEL SERIALIZABLE',
      'SELECT 1',
      'SELECT 2',
      'COMMIT',
    ])
    expect(clients[0].release).toHaveBeenCalledWith(undefined)
  })

  it('rolls back a failed batch and reports that no transaction is left', async () => {
    const { clients, messaging } = await loadApp({ failingSql: 'SELECT broken' })

    const response = (await messaging.send('database', 'database.batch', {
      destination: createDestination(),
      requestId: 'req-1',
      statements: [{ sql: 'SELECT broken' }, { sql: 'SELECT 1' }],
      commit: false,
    })) as DatabaseErrorResponse

    expect(response).toMatchObject({
      message: 'relation "missing" does not exist',
      requestId: 'req-1',
      connectionDiscarded: true,
    })
    expect(clients[0].queries.map((query) => query.sql)).toEqual([
      'BEGIN',
      'SELECT broken',
      'ROLLBACK',
    ])
    expect(clients[0].release).toHaveBeenCalledWith(undefined)
  })

  it('retries BEGIN of a batch on a broken pooled connection', async () => {
    const { clients, messaging } = await loadApp({ beginFailures: 1 })

    const response = await messaging.send('database', 'database.batch', {
      destination: createDestination(),
      statements: [{ sql: 'SELECT 1' }],
      commit: true,
    })

    expect(response).toEqual(okResponse)
    expect(clients).toHaveLength(2)
    expect(clients[0].release).toHaveBeenCalledWith(expect.any(Error))
    expect(clients[1].queries.map((query) => query.sql)).toEqual(['BEGIN', 'SELECT 1', 'COMMIT'])
  })

  it.each([
    [{ statements: [] }, 'statements must be a non-empty array'],
    [{ statements: [{ sql: 1 }] }, 'sql must be a string'],
    [{ statements: [{ sql: 'SELECT 1' }], commit: 'yes' }, 'commit must be a boolean'],
    [{ statements: [{ sql: 'SELECT 1', result: 1 }] }, 'result must be a boolean when present'],
  ])('rejects malformed batch requests %o', async (overrides, message) => {
    const { clients, messaging } = await loadApp()

    const defaults = {
      destination: createDestination(),
      statements: [{ sql: 'SELECT 1' }],
      commit: true,
    }
    const response = (await messaging.send('database', 'database.batch', {
      ...defaults,
      ...overrides,
    })) as DatabaseErrorResponse

    expect(response).toMatchObject({ code: 'PROTOCOL_ERROR', message })
    expect(clients).toHaveLength(0)
  })

  it('closes pools and rejects new work after shutdown starts', async () => {
    const { app, messaging, pools } = await loadApp()

    await messaging.send('database', 'database.query', {
      destination: createDestination(),
      sql: 'SELECT 1',
    })
    await app.close()

    const response = (await messaging.send('database', 'database.query', {
      destination: createDestination(),
      sql: 'SELECT 1',
    })) as DatabaseErrorResponse

    expect(pools[0].ended).toBe(true)
    expect(response).toMatchObject({
      code: 'SHUTDOWN',
      message: 'Database application is shutting down',
    })
  })
})

function createDestination(
  overrides: Partial<{
    connectionString: string
    id: string
    isExternalPool: boolean
    maxConnections: number
  }> = {}
) {
  return {
    connectionString: 'postgres://tenant-db',
    id: 'tenant-a',
    isExternalPool: false,
    maxConnections: 10,
    ...overrides,
  }
}
