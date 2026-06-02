import { afterEach, describe, expect, it, vi } from 'vitest'
import type { DatabaseErrorResponse } from '../errors.js'

type Handler = (data: unknown) => Promise<unknown>

type MockClient = {
  queries: Array<{ sql: string; values?: unknown[] }>
  query: ReturnType<typeof vi.fn>
  release: ReturnType<typeof vi.fn>
}

type LoadedApp = {
  app: typeof import('../index.js')
  clients: MockClient[]
  handlers: Map<string, Handler>
  pools: Array<{ config: Record<string, unknown>; ended: boolean; queries: Array<{ sql: string; values?: unknown[] }> }>
}

const originalEnv = { ...process.env }

afterEach(async () => {
  vi.doUnmock('pg')
  vi.doUnmock('pg/lib/connection')
  vi.resetModules()
  delete (globalThis as typeof globalThis & { platformatic?: unknown }).platformatic
  process.env = { ...originalEnv }
})

describe('database Watt application messaging handlers', () => {
  it('registers prefixed handlers and exposes no server', async () => {
    const { app, handlers } = await loadApp()

    expect(app.hasServer).toBe(false)
    expect([...handlers.keys()].sort()).toEqual([
      'database.acquire',
      'database.beginTransaction',
      'database.cancel',
      'database.commitTransaction',
      'database.lockedQuery',
      'database.query',
      'database.release',
      'database.rollbackTransaction',
      'database.test.resetStats',
      'database.test.stats',
    ])
  })

  it('executes stateless queries against the single-tenant destination', async () => {
    const { clients, handlers } = await loadApp()

    const response = await handlers.get('database.query')?.({
      destination: 'default',
      requestId: 'req-1',
      sql: 'SELECT 1',
      values: [1],
    })

    expect(response).toEqual({ rowCount: 1, rows: [{ ok: true }] })
    expect(clients[0].query).toHaveBeenCalledWith('SELECT 1', [1])
    expect(clients[0].release).toHaveBeenCalledWith(undefined)
  })

  it('returns validation errors from malformed requests', async () => {
    const { handlers } = await loadApp()

    const response = (await handlers.get('database.query')?.({
      destination: '',
      sql: 'SELECT 1',
    })) as DatabaseErrorResponse

    expect(response).toMatchObject({
      code: 'PROTOCOL_ERROR',
      message: 'destination must be a non-empty string',
    })
  })

  it('enforces result limits in handlers', async () => {
    const { handlers } = await loadApp({
      env: { DATABASE_WATT_MAX_RESULT_ROWS: '1' },
      queryRows: [{ id: 1 }, { id: 2 }],
    })

    const response = (await handlers.get('database.query')?.({
      destination: 'default',
      sql: 'SELECT 1',
    })) as DatabaseErrorResponse

    expect(response).toMatchObject({
      code: 'RESULT_TOO_LARGE',
      message: 'Result row limit exceeded',
    })
    expect(response).not.toHaveProperty('rows')
  })

  it('acquires and releases pinned connections', async () => {
    const { clients, handlers } = await loadApp()

    const acquire = (await handlers.get('database.acquire')?.({
      destination: 'default',
    })) as { lockId: string }

    expect(acquire.lockId).toBeTruthy()

    const release = await handlers.get('database.release')?.({ lockId: acquire.lockId })

    expect(release).toEqual({ released: true })
    expect(clients[0].release).toHaveBeenCalledWith(undefined)
  })

  it('runs transaction lifecycle on one pinned connection', async () => {
    const { clients, handlers } = await loadApp()

    const begin = (await handlers.get('database.beginTransaction')?.({
      destination: 'default',
      isolationLevel: 'serializable',
      readOnly: true,
    })) as { lockId: string }

    expect(begin.lockId).toBeTruthy()

    await handlers.get('database.lockedQuery')?.({
      lockId: begin.lockId,
      sql: 'SELECT 1',
    })
    const commit = await handlers.get('database.commitTransaction')?.({ lockId: begin.lockId })

    expect(commit).toEqual({ committed: true })
    expect(clients[0].queries.map((query) => query.sql)).toEqual([
      'BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY',
      `SELECT set_config('statement_timeout', $1, true)`,
      'SELECT 1',
      'COMMIT',
    ])
    expect(clients[0].release).toHaveBeenCalledWith(undefined)
  })

  it('resolves multitenant destinations through the master pool', async () => {
    const { clients, handlers, pools } = await loadApp({
      env: { DATABASE_MULTITENANT_URL: 'postgres://master', MULTI_TENANT: 'true' },
      tenantRows: [{ database_url: 'postgres://tenant-db', max_connections: 4 }],
    })

    const response = await handlers.get('database.query')?.({
      destination: 'tenant-a',
      sql: 'SELECT 1',
    })

    expect(response).toEqual({ rowCount: 1, rows: [{ ok: true }] })
    expect(pools[0].queries[0]).toMatchObject({
      values: ['tenant-a'],
    })
    expect(clients[0].query).toHaveBeenCalledWith('SELECT 1', undefined)
  })

  it('resolves the reserved master destination directly to the master database', async () => {
    const { clients, handlers, pools } = await loadApp({
      env: { DATABASE_MULTITENANT_URL: 'postgres://master', MULTI_TENANT: 'true' },
    })

    const response = await handlers.get('database.query')?.({
      destination: 'master',
      sql: 'SELECT 1',
    })

    expect(response).toEqual({ rowCount: 1, rows: [{ ok: true }] })
    expect(pools[0].config).toMatchObject({ connectionString: 'postgres://master' })
    expect(pools[0].queries).toHaveLength(0)
    expect(clients[0].query).toHaveBeenCalledWith('SELECT 1', undefined)
  })

  it('returns DESTINATION_UNKNOWN for the master destination without master config', async () => {
    const { handlers } = await loadApp({
      env: { DATABASE_MULTITENANT_URL: '', MULTI_TENANT: 'true' },
    })

    const response = (await handlers.get('database.query')?.({
      destination: 'master',
      sql: 'SELECT 1',
    })) as DatabaseErrorResponse

    expect(response).toMatchObject({
      code: 'DESTINATION_UNKNOWN',
      destination: 'master',
    })
  })

  it('returns DESTINATION_UNKNOWN for missing tenants', async () => {
    const { handlers } = await loadApp({
      env: { DATABASE_MULTITENANT_URL: 'postgres://master', MULTI_TENANT: 'true' },
      tenantRows: [],
    })

    const response = (await handlers.get('database.query')?.({
      destination: 'missing',
      sql: 'SELECT 1',
    })) as DatabaseErrorResponse

    expect(response).toMatchObject({
      code: 'DESTINATION_UNKNOWN',
      destination: 'missing',
    })
  })

  it('closes pools and rejects new work after shutdown starts', async () => {
    const { app, handlers, pools } = await loadApp()

    await handlers.get('database.query')?.({
      destination: 'default',
      sql: 'SELECT 1',
    })
    await app.close()

    const response = (await handlers.get('database.query')?.({
      destination: 'default',
      sql: 'SELECT 1',
    })) as DatabaseErrorResponse

    expect(pools[0].ended).toBe(true)
    expect(response).toMatchObject({
      code: 'SHUTDOWN',
      message: 'Database application is shutting down',
    })
  })
})

async function loadApp(options: {
  env?: Record<string, string>
  queryRows?: unknown[]
  tenantRows?: unknown[]
} = {}): Promise<LoadedApp> {
  vi.resetModules()

  const handlers = new Map<string, Handler>()
  const clients: MockClient[] = []
  const pools: LoadedApp['pools'] = []
  const queryRows = options.queryRows || [{ ok: true }]
  const tenantRows = options.tenantRows || []

  process.env = {
    ...originalEnv,
    DATABASE_URL: 'postgres://single-tenant',
    ...options.env,
  }

  ;(globalThis as typeof globalThis & { platformatic?: unknown }).platformatic = {
    messaging: {
      handle(message: string, handler: Handler) {
        handlers.set(message, handler)
      },
    },
  }

  vi.doMock('pg', () => {
    class MockDatabaseError extends Error {
      code?: string
      constructor(message: string) {
        super(message)
      }
    }

    class MockPool {
      totalCount = 0
      idleCount = 0
      waitingCount = 0
      ended = false
      queries: Array<{ sql: string; values?: unknown[] }> = []
      config: Record<string, unknown>

      constructor(config: Record<string, unknown>) {
        this.config = config
        pools.push(this)
      }

      async query(sql: string, values?: unknown[]) {
        this.queries.push({ sql, values })
        return { rowCount: tenantRows.length, rows: tenantRows }
      }

      async connect() {
        this.totalCount++
        const client = createMockClient(queryRows)
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
      default: { types: { setTypeParser: vi.fn() } },
    }
  })

  vi.doMock('pg/lib/connection', () => ({
    default: class MockPgConnection {},
  }))

  const app = await import('../index.js')
  return { app, clients, handlers, pools }
}

function createMockClient(rows: unknown[]): MockClient {
  const client: MockClient = {
    queries: [],
    query: vi.fn(async (sql: string, values?: unknown[]) => {
      client.queries.push({ sql, values })
      return { rowCount: rows.length, rows }
    }),
    release: vi.fn(),
  }

  return client
}
