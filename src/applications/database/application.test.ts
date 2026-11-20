import { setupLoopbackMessaging } from '@platformatic/runtime'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { DatabaseErrorResponse } from './errors.js'
import type { create } from './index.js'

type MockClient = {
  queries: Array<{ sql: string; values?: unknown[] }>
  query: ReturnType<typeof vi.fn>
  release: ReturnType<typeof vi.fn>
}

type MockedEnvironment = {
  app: ReturnType<typeof create>
  messaging: ReturnType<typeof setupLoopbackMessaging>
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

function createMockClient(rows: unknown[], failures: { begin: number }): MockClient {
  const client: MockClient = {
    queries: [],
    query: vi.fn(async (sql: string, values?: unknown[]) => {
      client.queries.push({ sql, values })
      if (sql.startsWith('BEGIN') && failures.begin > 0) {
        failures.begin--
        throw new Error('Connection terminated unexpectedly')
      }
      return { rowCount: rows.length, rows }
    }),
    release: vi.fn(),
  }

  return client
}

async function loadApp(
  options: { env?: Record<string, string>; queryRows?: unknown[]; beginFailures?: number } = {}
): Promise<MockedEnvironment> {
  vi.resetModules()

  const clients: MockClient[] = []
  const pools: MockedEnvironment['pools'] = []
  const queryRows = options.queryRows || [{ ok: true }]
  const failures = { begin: options.beginFailures ?? 0 }

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
  const app = create()
  return { app, messaging, clients, pools }
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

  it('executes stateless queries against the single-tenant destination', async () => {
    const { messaging, clients } = await loadApp()

    const response = await messaging.send('foo', 'database.query', {
      destination: createDestination(),
      requestId: 'req-1',
      sql: 'SELECT 1',
      values: [1],
    })

    expect(response).toEqual({ rowCount: 1, rows: [{ ok: true }] })
    expect(clients[0].query).toHaveBeenCalledWith('SELECT 1', [1])
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

    expect(response).toEqual({ rowCount: 1, rows: [{ ok: true }] })
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

    expect(response).toEqual({ rowCount: 1, rows: [{ ok: true }] })
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
