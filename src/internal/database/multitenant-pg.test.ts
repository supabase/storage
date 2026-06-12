import { afterEach, describe, it, vi } from 'vitest'

type MultitenantPgModule = typeof import('./multitenant-pg')
type MockPgPoolOptions = {
  connectionString?: string
  max?: number
}
type MockPgClient = {
  query: () => Promise<{ rows: unknown[] }>
  release: () => void
}
type MockPgPool = {
  options: MockPgPoolOptions
  ended: boolean
  on: ReturnType<typeof vi.fn>
  connect: () => Promise<MockPgClient>
  end: () => Promise<void>
}
type MockWattQuery = {
  destination: string
  statement: unknown
}

let createdPools: MockPgPool[] = []
let wattQueries: MockWattQuery[] = []
const originalEnv = { ...process.env }

async function loadMultitenantPgModule(
  configOverrides: Record<string, unknown> = {},
  options: { hasWattMessaging?: boolean } = {}
): Promise<MultitenantPgModule> {
  vi.resetModules()
  process.env = { ...originalEnv, MULTI_TENANT: 'true' }
  mockPgModule()
  mockWattConnectionModule()
  const globals = await import('@platformatic/globals')

  if (options.hasWattMessaging) {
    globals.updateGlobals({ messaging: {} as unknown as never })
  } else {
    globals.removeGlobals(['messaging'])
  }

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    databaseApplicationName: 'storage-test',
    multitenantDatabaseUrl: 'postgres://user:password@db.example.test:5432/postgres',
    multitenantDatabasePoolUrl: undefined,
    multitenantMaxConnections: 3,
    ...configOverrides,
  } as Parameters<typeof configModule.mergeConfig>[0])

  return import('./multitenant-pg')
}

describe('multitenant pg pool', () => {
  let loadedModule: MultitenantPgModule | undefined

  afterEach(async () => {
    await loadedModule?.closeMultitenantPg()
    loadedModule = undefined
    vi.doUnmock('pg')
    vi.doUnmock('./watt-connection')
    vi.resetModules()
    process.env = { ...originalEnv }
  })

  it('does not pass DATABASE_SSL_ROOT_CERT into the shared multitenant pool config', async () => {
    loadedModule = await loadMultitenantPgModule({
      databaseSSLRootCert: 'root-cert',
    })

    await runQuery(loadedModule)

    expect(getLatestPool().options).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@db.example.test:5432/postgres',
        max: 3,
      })
    )
    expect(getLatestPool().options).not.toHaveProperty('ssl')
  })

  it('uses the pool URL for sizing without injecting SSL settings', async () => {
    loadedModule = await loadMultitenantPgModule({
      databaseSSLRootCert: 'root-cert',
      multitenantDatabasePoolUrl: 'postgres://user:password@1.2.3.4:6432/postgres',
    })

    await runQuery(loadedModule)

    expect(getLatestPool().options).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@1.2.3.4:6432/postgres',
        max: 30,
      })
    )
    expect(getLatestPool().options).not.toHaveProperty('ssl')
  })

  it('installs an idle pg client error handler on the shared multitenant pool', async () => {
    loadedModule = await loadMultitenantPgModule()

    await runQuery(loadedModule)

    expect(getLatestPool().on).toHaveBeenCalledWith('error', expect.any(Function))
  })

  it('reads the current config after runtime config changes', async () => {
    loadedModule = await loadMultitenantPgModule()

    await runQuery(loadedModule)

    expect(getLatestPool().options).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@db.example.test:5432/postgres',
        max: 3,
      })
    )

    const configModule = await import('../../config')
    configModule.mergeConfig({
      multitenantDatabaseUrl: 'postgres://user:password@db2.example.test:5432/postgres',
      multitenantMaxConnections: 5,
    })

    await runQuery(loadedModule)

    expect(getLatestPool().options).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@db2.example.test:5432/postgres',
        max: 5,
      })
    )
  })

  it('replaces the current pool after runtime config changes', async () => {
    loadedModule = await loadMultitenantPgModule()

    await runQuery(loadedModule)
    const firstPool = getLatestPool()

    expect(firstPool.options).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@db.example.test:5432/postgres',
        max: 3,
      })
    )

    const configModule = await import('../../config')
    configModule.mergeConfig({
      multitenantDatabasePoolUrl: 'postgres://user:password@1.2.3.4:6432/postgres',
      multitenantMaxConnections: 4,
    })

    await runQuery(loadedModule)
    const secondPool = getLatestPool()

    expect(secondPool).not.toBe(firstPool)
    expect(firstPool.ended).toBe(true)
    expect(secondPool.options).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@1.2.3.4:6432/postgres',
        max: 40,
      })
    )
  })

  it('blocks new work while close is pending and allows reuse after close settles', async () => {
    loadedModule = await loadMultitenantPgModule()

    await runQuery(loadedModule)
    const pool = getLatestPool()
    const closeDeferred = Promise.withResolvers<void>()
    const endSpy = vi.spyOn(pool, 'end').mockReturnValue(closeDeferred.promise as never)

    const closePromise = loadedModule.closeMultitenantPg()

    try {
      await expect(loadedModule.multitenantPgExecutor.query('select 1')).rejects.toThrow(
        'MultitenantPgPool is closing'
      )
    } finally {
      closeDeferred.resolve()
      await closePromise
      endSpy.mockRestore()
    }

    await runQuery(loadedModule)

    expect(getLatestPool()).not.toBe(pool)
  })

  it('keeps shutdown terminal after the current pool is closed', async () => {
    loadedModule = await loadMultitenantPgModule()

    await runQuery(loadedModule)
    const pool = getLatestPool()

    await loadedModule.shutdownMultitenantPg()

    await expect(loadedModule.multitenantPgExecutor.query('select 1')).rejects.toThrow(
      'MultitenantPgPool is shut down'
    )
    expect(pool.ended).toBe(true)
  })

  it('does not export raw pool or pool config helpers', async () => {
    loadedModule = await loadMultitenantPgModule()

    expect('getMultitenantPgPool' in loadedModule).toBe(false)
    expect('getMultitenantPgPoolConfig' in loadedModule).toBe(false)
  })

  it('uses Database Watt for master queries when messaging is available', async () => {
    loadedModule = await loadMultitenantPgModule({}, { hasWattMessaging: true })

    await runQuery(loadedModule)

    expect(createdPools).toHaveLength(0)
    expect(wattQueries).toEqual([
      {
        destination: 'master',
        statement: 'select 1',
      },
    ])
  })

  it('uses Database Watt for master transactions when messaging is available', async () => {
    loadedModule = await loadMultitenantPgModule({}, { hasWattMessaging: true })

    const tx = await loadedModule.multitenantPgExecutor.beginTransaction()
    await tx.query('select 1')
    await tx.commit()

    expect(createdPools).toHaveLength(0)
    expect(wattQueries).toEqual([
      {
        destination: 'master',
        statement: 'select 1',
      },
    ])
  })
})

async function runQuery(module: MultitenantPgModule): Promise<void> {
  await module.multitenantPgExecutor.query('select 1')
}

function getLatestPool(): MockPgPool {
  const pool = createdPools.at(-1)

  if (!pool) {
    throw new Error('Expected pg Pool to be created')
  }

  return pool
}

function mockPgModule(): void {
  createdPools = []

  vi.doMock('pg', () => {
    const types = {
      setTypeParser: vi.fn(),
    }

    class DatabaseError extends Error {}

    class MockPool implements MockPgPool {
      readonly options: MockPgPoolOptions
      ended = false
      on = vi.fn()
      connect = vi.fn(async () => createMockPgClient())
      end = vi.fn(async () => {
        this.ended = true
      })

      constructor(options: MockPgPoolOptions) {
        this.options = options
        createdPools.push(this)
      }
    }

    return {
      DatabaseError,
      Pool: MockPool,
      types,
      default: {
        DatabaseError,
        Pool: MockPool,
        types,
      },
    }
  })
}

function mockWattConnectionModule(): void {
  wattQueries = []

  vi.doMock('./watt-connection', () => {
    class MockDatabaseWattPgExecutor {
      constructor(private readonly destination: string) {}

      async query(statement: unknown) {
        wattQueries.push({ destination: this.destination, statement })
        return { rowCount: 0, rows: [] }
      }

      async beginTransaction() {
        return {
          commit: vi.fn(async () => undefined),
          isCompleted: vi.fn(() => false),
          query: vi.fn(async (statement: unknown) => {
            wattQueries.push({ destination: this.destination, statement })
            return { rowCount: 0, rows: [] }
          }),
          rollback: vi.fn(async () => undefined),
        }
      }
    }

    return {
      DatabaseWattPgExecutor: MockDatabaseWattPgExecutor,
    }
  })
}

function createMockPgClient(): MockPgClient {
  return {
    query: vi.fn(async () => ({ rows: [] })),
    release: vi.fn(),
  }
}
