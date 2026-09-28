import Fastify, { type FastifyRequest } from 'fastify'
import { vi } from 'vitest'
import { MultitenantMigrationStrategy } from '../../config'

afterEach(() => {
  vi.doUnmock('@internal/database')
  vi.doUnmock('@internal/database/migrations')
  vi.resetModules()
})

async function loadDbPlugins({
  databaseEnableQueryCancellation = false,
  dbMigration = { initialmigration: 1, 'search-v2': 27 },
  dbMigrationStrategy = MultitenantMigrationStrategy.PROGRESSIVE,
  isMultitenant = false,
}: {
  databaseEnableQueryCancellation?: boolean
  dbMigration?: Record<string, number>
  dbMigrationStrategy?: MultitenantMigrationStrategy
  isMultitenant?: boolean
} = {}) {
  vi.resetModules()

  const requestDb = {
    dispose: vi.fn(),
    setAbortSignal: vi.fn(),
  }
  const getPostgresConnection = vi.fn().mockResolvedValue(requestDb)
  const getServiceKeyUser = vi.fn().mockResolvedValue({
    jwt: 'service-jwt',
    payload: {
      role: 'service_role',
    },
  })
  const getTenantConfig = vi.fn()
  const deleteTenantConfig = vi.fn()
  const areMigrationsUpToDate = vi.fn()
  const lastLocalMigrationName = vi.fn().mockResolvedValue('initialmigration')
  const highestLocalMigrationName = vi.fn().mockReturnValue('initialmigration')
  const runMigrationsOnTenant = vi.fn().mockImplementation(() => lastLocalMigrationName())
  const completeTenantMigrations = vi.fn().mockResolvedValue(1)
  const progressiveMigrations = {
    addTenant: vi.fn(),
  }

  vi.doMock('@internal/database', () => {
    return {
      getPostgresConnection,
      getServiceKeyUser,
      getTenantConfig,
      deleteTenantConfig,
      PgTenantConnection: class {},
      TenantMigrationStatus: {
        COMPLETED: 'COMPLETED',
      },
    }
  })

  vi.doMock('@internal/database/migrations', () => {
    return {
      areMigrationsUpToDate,
      DBMigration: dbMigration,
      lastLocalMigrationName,
      highestLocalMigrationName,
      isDBMigrationName: (value: unknown) =>
        typeof value === 'string' && Object.hasOwn(dbMigration, value),
      isUnrecognizedMigration: (value: unknown) =>
        typeof value === 'string' && value.length > 0 && !Object.hasOwn(dbMigration, value),
      progressiveMigrations,
      runMigrationsOnTenant,
      completeTenantMigrations,
    }
  })

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    databaseEnableQueryCancellation,
    dbMigrationStrategy,
    isMultitenant,
  })

  const { db, dbSuperUser } = await import('./db')

  return {
    db,
    dbSuperUser,
    areMigrationsUpToDate,
    getPostgresConnection,
    getTenantConfig,
    deleteTenantConfig,
    lastLocalMigrationName,
    highestLocalMigrationName,
    progressiveMigrations,
    requestDb,
    runMigrationsOnTenant,
    completeTenantMigrations,
  }
}

describe('dbSuperUser plugin', () => {
  it('does not forward route-level maxConnections into shared tenant pool settings', async () => {
    const { dbSuperUser, getPostgresConnection } = await loadDbPlugins()
    const app = Fastify()

    app.decorateRequest('tenantId')
    app.addHook('onRequest', async (request) => {
      request.tenantId = 'tenant-id'
    })
    const legacyOptions = { disableHostCheck: true, maxConnections: 5 } as unknown as {
      disableHostCheck: boolean
    }
    await app.register(dbSuperUser, legacyOptions)
    app.get('/test', async () => ({ ok: true }))

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/test',
        headers: {
          'x-forwarded-host': 'tenant.local.test',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(getPostgresConnection).toHaveBeenCalledTimes(1)
      expect(getPostgresConnection.mock.calls[0][0]).not.toHaveProperty('maxConnections')
    } finally {
      await app.close()
    }
  })
})

describe('migrations plugin', () => {
  function loadMigrationPlugins(options: Parameters<typeof loadDbPlugins>[0] = {}) {
    return loadDbPlugins({
      isMultitenant: true,
      dbMigrationStrategy: MultitenantMigrationStrategy.ON_REQUEST,
      ...options,
    })
  }

  async function buildMigrationApp(
    plugins: Awaited<ReturnType<typeof loadDbPlugins>>,
    getTenantId: (request: FastifyRequest) => string = () => 'tenant-id'
  ) {
    const app = Fastify()

    app.decorateRequest('tenantId')
    app.addHook('onRequest', async (request) => {
      request.tenantId = getTenantId(request)
    })
    await app.register(plugins.dbSuperUser)
    app.get('/test', async (request) => ({ latestMigration: request.latestMigration }))

    const injectTenant = (headers: Record<string, string> = {}) =>
      app.inject({ method: 'GET', url: '/test', headers })

    return { app, injectTenant }
  }

  function waitForImmediate() {
    return new Promise((resolve) => setImmediate(resolve))
  }

  it('refreshes the migration version for the request that completes migrations', async () => {
    const plugins = await loadMigrationPlugins()
    const tenant = {
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'initialmigration',
      syncMigrationsDone: false,
    }

    plugins.getTenantConfig.mockResolvedValue(tenant)
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.lastLocalMigrationName.mockResolvedValue('search-v2')
    plugins.runMigrationsOnTenant.mockResolvedValue('search-v2')
    plugins.completeTenantMigrations.mockResolvedValue(1)

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const response = await injectTenant()

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ latestMigration: 'search-v2' })
      expect(plugins.deleteTenantConfig).toHaveBeenCalledWith('tenant-id')
      expect(plugins.deleteTenantConfig.mock.invocationCallOrder[0]).toBeLessThan(
        plugins.getTenantConfig.mock.invocationCallOrder[2]
      )
    } finally {
      await app.close()
    }
  })

  it('reloads the winning snapshot when migration completion loses its version compare', async () => {
    const plugins = await loadMigrationPlugins({
      dbMigration: {
        initialmigration: 1,
        'search-v2': 27,
        'search-v2-optimised': 50,
      },
    })
    const initialTenant = {
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'initialmigration',
      migrationStatus: 'COMPLETED',
      syncMigrationsDone: false,
    }
    const winningTenant = {
      ...initialTenant,
      migrationVersion: 'search-v2-optimised',
    }
    plugins.getTenantConfig
      .mockResolvedValueOnce(initialTenant)
      .mockResolvedValueOnce(initialTenant)
      .mockResolvedValueOnce(initialTenant)
      .mockResolvedValue(winningTenant)
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.lastLocalMigrationName.mockResolvedValue('search-v2')
    plugins.runMigrationsOnTenant.mockResolvedValue('search-v2')
    plugins.completeTenantMigrations.mockResolvedValue(0)

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const response = await injectTenant()

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ latestMigration: 'search-v2-optimised' })
      expect(plugins.completeTenantMigrations).toHaveBeenCalledWith('tenant-id', {
        expectedMigrationVersion: 'initialmigration',
        migration: 'search-v2',
      })
      expect(plugins.deleteTenantConfig).toHaveBeenCalledTimes(2)
      expect(winningTenant.migrationVersion).toBe('search-v2-optimised')
    } finally {
      await app.close()
    }
  })

  it('keeps an applied migration version ahead of the local freeze target', async () => {
    const plugins = await loadMigrationPlugins({
      dbMigration: {
        initialmigration: 1,
        'search-v2': 27,
        'search-v2-optimised': 50,
      },
    })

    plugins.getTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'search-v2-optimised',
      syncMigrationsDone: true,
    })
    plugins.lastLocalMigrationName.mockResolvedValue('search-v2')

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const response = await injectTenant()

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ latestMigration: 'search-v2-optimised' })
    } finally {
      await app.close()
    }
  })

  it.each(
    Object.values(MultitenantMigrationStrategy).flatMap((strategy) =>
      ['FAILED', 'FAILED_STALE', null].map((migrationStatus) => ({ strategy, migrationStatus }))
    )
  )('serves the observed ledger when control is ahead with status $migrationStatus under $strategy', async ({
    strategy,
    migrationStatus,
  }) => {
    const plugins = await loadMigrationPlugins({
      dbMigrationStrategy: strategy,
      dbMigration: {
        initialmigration: 1,
        'search-v2': 27,
        'search-v2-optimised': 50,
      },
    })
    const tenant = {
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'search-v2-optimised',
      migrationStatus,
      syncMigrationsDone: false,
    }

    plugins.getTenantConfig.mockResolvedValue(tenant)
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.lastLocalMigrationName.mockResolvedValue('search-v2')
    plugins.runMigrationsOnTenant.mockResolvedValue('search-v2')
    plugins.completeTenantMigrations.mockResolvedValue(1)

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const response = await injectTenant()

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ latestMigration: 'search-v2' })
      expect(plugins.completeTenantMigrations).toHaveBeenCalledWith('tenant-id', {
        expectedMigrationVersion: 'search-v2-optimised',
        migration: 'search-v2',
      })
      const cached = await injectTenant()
      expect(cached.json()).toEqual({ latestMigration: 'search-v2' })
      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(1)
      expect(plugins.progressiveMigrations.addTenant).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('keeps an unrecognized migration version at the highest local migration under ON_REQUEST', async () => {
    const plugins = await loadMigrationPlugins({
      dbMigration: {
        initialmigration: 1,
        'search-v2': 27,
        'search-v2-optimised': 50,
      },
    })
    const tenant = {
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'a-migration-this-binary-does-not-know',
      migrationStatus: 'COMPLETED',
      syncMigrationsDone: false,
    }

    plugins.getTenantConfig.mockResolvedValue(tenant)
    plugins.areMigrationsUpToDate.mockResolvedValue(true)
    plugins.lastLocalMigrationName.mockResolvedValue('search-v2')
    plugins.highestLocalMigrationName.mockReturnValue('search-v2-optimised')

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const first = await injectTenant()
      expect(first.statusCode).toBe(200)
      expect(first.json()).toEqual({ latestMigration: 'search-v2-optimised' })

      const second = await injectTenant()
      expect(second.statusCode).toBe(200)
      expect(second.json()).toEqual({ latestMigration: 'search-v2-optimised' })

      expect(plugins.areMigrationsUpToDate).toHaveBeenCalledTimes(1)
      expect(plugins.runMigrationsOnTenant).not.toHaveBeenCalled()
      expect(plugins.completeTenantMigrations).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('does not serve or certify a missing physical ledger position', async () => {
    const plugins = await loadMigrationPlugins()
    plugins.getTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'initialmigration',
    })
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.runMigrationsOnTenant.mockResolvedValue(undefined)
    const { app, injectTenant } = await buildMigrationApp(plugins)
    try {
      expect((await injectTenant()).statusCode).toBe(500)
      expect(plugins.completeTenantMigrations).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('clamps an unrecognized physical ledger when repairing a missing snapshot', async () => {
    const plugins = await loadMigrationPlugins({
      dbMigrationStrategy: MultitenantMigrationStrategy.PROGRESSIVE,
    })
    plugins.getTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: null,
      syncMigrationsDone: false,
    })
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.lastLocalMigrationName.mockResolvedValue('initialmigration')
    plugins.highestLocalMigrationName.mockReturnValue('search-v2')
    plugins.runMigrationsOnTenant.mockResolvedValue('future-migration')

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const response = await injectTenant()

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ latestMigration: 'search-v2' })
      expect(plugins.completeTenantMigrations).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it.each([
    ['COMPLETED', 0, true],
    ['FAILED', 1, true],
  ])('handles a future %s version from the second hook without overwriting it', async (migrationStatus, migrationRuns, syncMigrationsDone) => {
    const plugins = await loadMigrationPlugins()
    const futureTenant = {
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'future-migration',
      migrationStatus,
      syncMigrationsDone: false,
    }
    plugins.getTenantConfig
      .mockResolvedValueOnce({
        databaseUrl: 'postgres://tenant-db',
        migrationVersion: 'initialmigration',
        syncMigrationsDone: false,
      })
      .mockResolvedValue(futureTenant)
    plugins.areMigrationsUpToDate.mockResolvedValue(migrationStatus === 'COMPLETED')
    plugins.lastLocalMigrationName.mockResolvedValue('search-v2')
    plugins.highestLocalMigrationName.mockReturnValue('search-v2')
    plugins.runMigrationsOnTenant.mockResolvedValue('future-migration')

    const { app, injectTenant } = await buildMigrationApp(plugins)
    try {
      const response = await injectTenant()
      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ latestMigration: 'search-v2' })
      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(migrationRuns)
      expect(plugins.completeTenantMigrations).not.toHaveBeenCalled()
      expect(futureTenant).toEqual({
        databaseUrl: 'postgres://tenant-db',
        migrationVersion: 'future-migration',
        migrationStatus,
        syncMigrationsDone,
      })
    } finally {
      await app.close()
    }
  })

  it.each([
    {
      kind: 'recognized',
      refreshedVersion: 'search-v2-optimised',
      refreshedStatus: 'COMPLETED',
      expectedSnapshot: 'search-v2-optimised',
      expectedCompletion: 'search-v2',
    },
    {
      kind: 'unrecognized',
      refreshedVersion: 'future-migration',
      refreshedStatus: 'FAILED',
      expectedSnapshot: 'search-v2',
      expectedCompletion: 'search-v2',
    },
  ])('shares a $kind migration observed by the post-run refresh with every waiting request', async ({
    refreshedVersion,
    refreshedStatus,
    expectedSnapshot,
    expectedCompletion,
  }) => {
    const plugins = await loadMigrationPlugins({
      dbMigration: {
        initialmigration: 1,
        'search-v2': 27,
        'search-v2-optimised': 50,
      },
    })
    const initialTenant = {
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'initialmigration',
      migrationStatus: 'COMPLETED',
      syncMigrationsDone: false,
    }
    const refreshedTenant = {
      ...initialTenant,
      migrationVersion: refreshedVersion,
      migrationStatus: refreshedStatus,
    }
    const migration = Promise.withResolvers<void>()
    let migrationFinished = false

    plugins.getTenantConfig.mockImplementation(async () =>
      migrationFinished ? refreshedTenant : initialTenant
    )
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.lastLocalMigrationName.mockResolvedValue('search-v2')
    plugins.highestLocalMigrationName.mockReturnValue('search-v2')
    plugins.runMigrationsOnTenant.mockImplementation(async () => {
      await migration.promise
      migrationFinished = true
      return 'search-v2'
    })
    plugins.completeTenantMigrations.mockResolvedValue(0)

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const first = injectTenant()
      const second = injectTenant()

      await waitForImmediate()
      expect(plugins.areMigrationsUpToDate).toHaveBeenCalledTimes(1)
      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(1)
      migration.resolve()

      const responses = await Promise.all([first, second])

      expect(responses.map((response) => response.json())).toEqual([
        { latestMigration: expectedSnapshot },
        { latestMigration: expectedSnapshot },
      ])
      if (expectedCompletion) {
        expect(plugins.completeTenantMigrations).toHaveBeenCalledWith('tenant-id', {
          expectedMigrationVersion: 'initialmigration',
          migration: expectedCompletion,
        })
      } else {
        expect(plugins.completeTenantMigrations).not.toHaveBeenCalled()
      }
      expect(refreshedTenant).toEqual({
        databaseUrl: 'postgres://tenant-db',
        migrationVersion: refreshedVersion,
        migrationStatus: refreshedStatus,
        syncMigrationsDone: true,
      })
      const cached = await injectTenant()
      expect(cached.statusCode).toBe(200)
      expect(cached.json()).toEqual({ latestMigration: expectedSnapshot })
      expect(plugins.areMigrationsUpToDate).toHaveBeenCalledTimes(1)
      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(1)
    } finally {
      await app.close()
    }
  })

  it('shares the on-request migration check across route scopes', async () => {
    const plugins = await loadMigrationPlugins()
    const tenant = {
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'initialmigration',
      syncMigrationsDone: false,
    }
    const migration = Promise.withResolvers<void>()

    plugins.getTenantConfig.mockResolvedValue(tenant)
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.runMigrationsOnTenant.mockImplementation(async () => {
      await migration.promise
      return 'initialmigration'
    })
    plugins.completeTenantMigrations.mockResolvedValue(1)

    // Two separate Fastify apps stand in for two route scopes, each registering
    // its own copy of the migrations plugin against the same module state.
    const firstScope = await buildMigrationApp(plugins)
    const secondScope = await buildMigrationApp(plugins)

    try {
      const first = firstScope.injectTenant()
      const second = secondScope.injectTenant()

      await waitForImmediate()

      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(1)

      migration.resolve()

      await expect(Promise.all([first, second])).resolves.toEqual([
        expect.objectContaining({ statusCode: 200 }),
        expect.objectContaining({ statusCode: 200 }),
      ])
    } finally {
      await firstScope.app.close()
      await secondScope.app.close()
    }
  })

  it('shares migration failures across concurrent same-tenant requests and retries later', async () => {
    const plugins = await loadMigrationPlugins()
    const tenant = {
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'initialmigration',
      syncMigrationsDone: false,
    }
    const migration = Promise.withResolvers<void>()

    plugins.getTenantConfig.mockResolvedValue(tenant)
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.runMigrationsOnTenant
      .mockReturnValueOnce(migration.promise)
      .mockResolvedValueOnce('initialmigration')
    plugins.completeTenantMigrations.mockResolvedValue(1)

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const first = injectTenant()
      const second = injectTenant()

      await waitForImmediate()

      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(1)

      migration.reject(new Error('migration failed'))

      const failedResponses = await Promise.all([first, second])
      expect(failedResponses).toEqual([
        expect.objectContaining({ statusCode: 500 }),
        expect.objectContaining({ statusCode: 500 }),
      ])
      expect(tenant.syncMigrationsDone).toBe(false)

      const retry = await injectTenant()

      expect(retry.statusCode).toBe(200)
      expect(plugins.areMigrationsUpToDate).toHaveBeenCalledTimes(2)
      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(2)
      expect(tenant.syncMigrationsDone).toBe(true)
    } finally {
      await app.close()
    }
  })

  it('skips on-request migration checks when the tenant is already marked migrated', async () => {
    const plugins = await loadMigrationPlugins()

    plugins.getTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'initialmigration',
      syncMigrationsDone: true,
    })
    plugins.lastLocalMigrationName.mockResolvedValue('search-v2')

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const response = await injectTenant()

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ latestMigration: 'search-v2' })
      expect(plugins.areMigrationsUpToDate).not.toHaveBeenCalled()
      expect(plugins.runMigrationsOnTenant).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('skips migration execution when tenant migrations are already up to date', async () => {
    const plugins = await loadMigrationPlugins()

    plugins.getTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'initialmigration',
      syncMigrationsDone: false,
    })
    plugins.areMigrationsUpToDate.mockResolvedValue(true)

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const response = await injectTenant()

      expect(response.statusCode).toBe(200)
      expect(plugins.areMigrationsUpToDate).toHaveBeenCalledTimes(1)
      expect(plugins.runMigrationsOnTenant).not.toHaveBeenCalled()

      const secondResponse = await injectTenant()

      expect(secondResponse.statusCode).toBe(200)
      expect(plugins.areMigrationsUpToDate).toHaveBeenCalledTimes(1)
      expect(plugins.runMigrationsOnTenant).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('does not coalesce on-request migrations for different tenants', async () => {
    const plugins = await loadMigrationPlugins()
    const migrations = {
      'tenant-a': Promise.withResolvers<void>(),
      'tenant-b': Promise.withResolvers<void>(),
    }

    plugins.getTenantConfig.mockImplementation(async (tenantId: string) => ({
      databaseUrl: `postgres://${tenantId}`,
      migrationVersion: 'initialmigration',
      syncMigrationsDone: false,
    }))
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.runMigrationsOnTenant.mockImplementation(
      async ({ tenantId }: { tenantId: keyof typeof migrations }) => {
        await migrations[tenantId].promise
        return 'initialmigration'
      }
    )
    plugins.completeTenantMigrations.mockResolvedValue(1)

    const { app, injectTenant } = await buildMigrationApp(
      plugins,
      (request) => request.headers['x-tenant-id'] as string
    )

    try {
      const first = injectTenant({ 'x-tenant-id': 'tenant-a' })
      const second = injectTenant({ 'x-tenant-id': 'tenant-b' })

      await waitForImmediate()

      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(2)
      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledWith(
        expect.objectContaining({ tenantId: 'tenant-a' })
      )
      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledWith(
        expect.objectContaining({ tenantId: 'tenant-b' })
      )

      migrations['tenant-a'].resolve()
      migrations['tenant-b'].resolve()

      await expect(Promise.all([first, second])).resolves.toEqual([
        expect.objectContaining({ statusCode: 200 }),
        expect.objectContaining({ statusCode: 200 }),
      ])
    } finally {
      await app.close()
    }
  })
})

describe.each([
  {
    name: 'db plugin',
    register: async (
      app: ReturnType<typeof Fastify>,
      plugins: Awaited<ReturnType<typeof loadDbPlugins>>
    ) => {
      app.addHook('onRequest', async (request: FastifyRequest) => {
        request.jwt = 'user-jwt'
        request.jwtPayload = {
          role: 'authenticated',
        }
      })
      await app.register(plugins.db)
    },
  },
  {
    name: 'dbSuperUser plugin',
    register: async (
      app: ReturnType<typeof Fastify>,
      plugins: Awaited<ReturnType<typeof loadDbPlugins>>
    ) => {
      await app.register(plugins.dbSuperUser)
    },
  },
])('$name query cancellation signal wiring', ({ register }) => {
  it('does not materialize the disconnect signal when query cancellation is disabled', async () => {
    const plugins = await loadDbPlugins({ databaseEnableQueryCancellation: false })
    const app = Fastify()

    app.decorateRequest('tenantId')
    app.decorateRequest('signals')
    app.addHook('onRequest', async (request) => {
      request.tenantId = 'tenant-id'
      request.signals = {
        get disconnect(): AbortController {
          throw new Error('disconnect signal should stay lazy')
        },
      } as typeof request.signals
    })
    await register(app, plugins)
    app.get('/test', async () => ({ ok: true }))

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/test',
        headers: {
          'x-forwarded-host': 'tenant.local.test',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(plugins.requestDb.setAbortSignal).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('materializes the disconnect signal when query cancellation is enabled', async () => {
    const plugins = await loadDbPlugins({ databaseEnableQueryCancellation: true })
    const app = Fastify()
    const disconnectController = new AbortController()
    const disconnectGetter = vi.fn(() => disconnectController)

    app.decorateRequest('tenantId')
    app.decorateRequest('signals')
    app.addHook('onRequest', async (request) => {
      request.tenantId = 'tenant-id'
      request.signals = Object.defineProperty({}, 'disconnect', {
        get: disconnectGetter,
      }) as typeof request.signals
    })
    await register(app, plugins)
    app.get('/test', async () => ({ ok: true }))

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/test',
        headers: {
          'x-forwarded-host': 'tenant.local.test',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(disconnectGetter).toHaveBeenCalledTimes(1)
      expect(plugins.requestDb.setAbortSignal).toHaveBeenCalledWith(disconnectController.signal)
    } finally {
      await app.close()
    }
  })

  it('does not require request signals when query cancellation is enabled', async () => {
    const plugins = await loadDbPlugins({ databaseEnableQueryCancellation: true })
    const app = Fastify()

    app.decorateRequest('tenantId')
    app.addHook('onRequest', async (request) => {
      request.tenantId = 'tenant-id'
    })
    await register(app, plugins)
    app.get('/test', async () => ({ ok: true }))

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/test',
        headers: {
          'x-forwarded-host': 'tenant.local.test',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(plugins.requestDb.setAbortSignal).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })
})
