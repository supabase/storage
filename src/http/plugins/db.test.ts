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
  dbMigrationStrategy = MultitenantMigrationStrategy.PROGRESSIVE,
  isMultitenant = false,
}: {
  databaseEnableQueryCancellation?: boolean
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
  const { isDBMigrationName, isUnrecognizedMigration } = await vi.importActual<
    typeof import('@internal/database/migrations/guards')
  >('@internal/database/migrations/guards')
  const cacheTenantMigration = vi.fn().mockImplementation((tenant, applied) => {
    tenant.observedMigrationName = applied
    tenant.observedMigration = Promise.resolve(
      isDBMigrationName(applied) ? applied : highestLocalMigrationName()
    )
  })
  const observeTenantMigration = vi.fn((_tenantId, tenant) => tenant.observedMigration)
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

  vi.doMock('@internal/database/migrations', async () => {
    return {
      ...(await vi.importActual('@internal/database/migrations/guards')),
      ...(await vi.importActual('@internal/database/migrations/types')),
      areMigrationsUpToDate,
      lastLocalMigrationName,
      highestLocalMigrationName,
      cacheTenantMigration,
      getCachedTenantMigration: vi.fn((tenant) => tenant.observedMigration),
      needsObservation: vi.fn(
        (tenant) =>
          !isDBMigrationName(tenant.migrationVersion) ||
          tenant.migrationStatus !== 'COMPLETED' ||
          isUnrecognizedMigration(tenant.observedMigrationName)
      ),
      progressiveMigrations,
      runMigrationsOnTenant,
      completeTenantMigrations,
      observeTenantMigration,
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
    cacheTenantMigration,
    observeTenantMigration,
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
    const { setErrorHandler } = await import('../error-handler')
    setErrorHandler(app)

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

  function tenantRow<T extends Record<string, unknown>>(overrides: T = {} as T) {
    return {
      databaseUrl: 'postgres://tenant-db',
      databaseUrlEncrypted: 'encrypted-tenant-db',
      migrationVersion: 'initialmigration',
      migrationStatus: 'COMPLETED',
      syncMigrationsDone: false,
      ...overrides,
    }
  }

  function waitForImmediate() {
    return new Promise((resolve) => setImmediate(resolve))
  }

  function modelControlRow(
    plugins: Awaited<ReturnType<typeof loadDbPlugins>>,
    row: Record<string, unknown>
  ) {
    let current = row
    plugins.getTenantConfig.mockImplementation(async () => current)
    plugins.completeTenantMigrations.mockImplementation(
      async (_tenantId: string, { migration }: { migration: string }) => {
        current = { ...current, migrationVersion: migration, migrationStatus: 'COMPLETED' }
        return 1
      }
    )
  }

  it('refreshes the migration version for the request that completes migrations', async () => {
    const plugins = await loadMigrationPlugins()
    const tenant = tenantRow()

    modelControlRow(plugins, tenant)
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.lastLocalMigrationName.mockResolvedValue('search-v2')
    plugins.runMigrationsOnTenant.mockResolvedValue('search-v2')

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const response = await injectTenant()

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ latestMigration: 'search-v2' })
      expect(plugins.deleteTenantConfig).toHaveBeenCalledOnce()
      expect(plugins.deleteTenantConfig).toHaveBeenCalledWith('tenant-id')
      expect(plugins.completeTenantMigrations.mock.invocationCallOrder[0]).toBeLessThan(
        plugins.deleteTenantConfig.mock.invocationCallOrder[0]
      )
      expect(plugins.deleteTenantConfig.mock.invocationCallOrder[0]).toBeLessThan(
        plugins.getTenantConfig.mock.invocationCallOrder[1]
      )
    } finally {
      await app.close()
    }
  })

  it('serves the cached flight result without migration checks', async () => {
    const latestMigration = 'search-v2-optimised'
    const plugins = await loadMigrationPlugins()

    plugins.getTenantConfig.mockResolvedValue(
      tenantRow({ syncMigrationsDone: true, observedMigration: Promise.resolve(latestMigration) })
    )
    plugins.lastLocalMigrationName.mockResolvedValue('search-v2')

    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      const response = await injectTenant()

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ latestMigration })
      expect(plugins.areMigrationsUpToDate).not.toHaveBeenCalled()
      expect(plugins.runMigrationsOnTenant).not.toHaveBeenCalled()
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
    },
    {
      kind: 'unrecognized',
      refreshedVersion: 'future-migration',
      refreshedStatus: 'FAILED',
      expectedSnapshot: 'search-v2',
    },
  ])('shares a $kind migration observed by the post-run refresh with every waiting request', async ({
    kind,
    refreshedVersion,
    refreshedStatus,
    expectedSnapshot,
  }) => {
    const plugins = await loadMigrationPlugins()
    const initialTenant = tenantRow({
      migrationStatus: 'COMPLETED',
      observedMigration: undefined as Promise<string> | undefined,
    })
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
      expect(plugins.completeTenantMigrations).toHaveBeenCalledWith('tenant-id', {
        expectedMigrationVersion: 'initialmigration',
        expectedDatabaseUrl: 'encrypted-tenant-db',
        migration: 'search-v2',
      })
      expect(refreshedTenant).toEqual({
        databaseUrl: 'postgres://tenant-db',
        databaseUrlEncrypted: 'encrypted-tenant-db',
        migrationVersion: refreshedVersion,
        migrationStatus: refreshedStatus,
        syncMigrationsDone: kind === 'recognized',
        observedMigration: kind === 'recognized' ? expect.any(Promise) : undefined,
        ...(kind === 'recognized' ? { observedMigrationName: expectedSnapshot } : {}),
      })
      if (kind === 'recognized') {
        await expect(refreshedTenant.observedMigration).resolves.toBe(expectedSnapshot)
      }
      const cached = await injectTenant()
      expect(cached.statusCode).toBe(200)
      expect(cached.json()).toEqual({ latestMigration: expectedSnapshot })
      expect(plugins.areMigrationsUpToDate).toHaveBeenCalledTimes(kind === 'recognized' ? 1 : 2)
      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(kind === 'recognized' ? 1 : 2)
    } finally {
      await app.close()
    }
  })

  it('shares migration failures across route scopes for the same tenant and retries later', async () => {
    const plugins = await loadMigrationPlugins()
    const tenant = tenantRow()
    const migration = Promise.withResolvers<void>()

    plugins.getTenantConfig.mockResolvedValue(tenant)
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.runMigrationsOnTenant
      .mockReturnValueOnce(migration.promise)
      .mockResolvedValueOnce('initialmigration')
    plugins.completeTenantMigrations.mockResolvedValue(1)

    // Separate plugin registrations must share the same tenant flight.
    const firstScope = await buildMigrationApp(plugins)
    const secondScope = await buildMigrationApp(plugins)

    try {
      const first = firstScope.injectTenant()
      const second = secondScope.injectTenant()

      await waitForImmediate()

      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(1)

      migration.reject(new Error('migration failed'))

      const failedResponses = await Promise.all([first, second])
      expect(failedResponses).toEqual([
        expect.objectContaining({ statusCode: 500 }),
        expect.objectContaining({ statusCode: 500 }),
      ])
      expect(tenant.syncMigrationsDone).toBe(false)

      const retry = await secondScope.injectTenant()

      expect(retry.statusCode).toBe(200)
      expect(plugins.areMigrationsUpToDate).toHaveBeenCalledTimes(2)
      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(2)
      expect(tenant.syncMigrationsDone).toBe(true)
    } finally {
      await firstScope.app.close()
      await secondScope.app.close()
    }
  })

  it('runs the on-request migration when the tenant is marked synced but has no recorded version', async () => {
    const plugins = await loadMigrationPlugins()
    plugins.getTenantConfig.mockResolvedValue(
      tenantRow({ migrationVersion: undefined, syncMigrationsDone: true })
    )
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      expect((await injectTenant()).statusCode).toBe(200)
      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledTimes(1)
    } finally {
      await app.close()
    }
  })

  it.each([
    [null, null],
    ['initialmigration', 'COMPLETED'],
  ])('keeps a zero-row completion at an unknown head eligible for revalidation with %s / %s', async (migrationVersion, migrationStatus) => {
    const plugins = await loadMigrationPlugins()
    const tenant = {
      ...tenantRow(),
      migrationVersion,
      migrationStatus,
      observedMigration: undefined as Promise<string> | undefined,
    }
    plugins.getTenantConfig.mockResolvedValue(tenant)
    plugins.areMigrationsUpToDate.mockResolvedValueOnce(false).mockResolvedValue(true)
    plugins.highestLocalMigrationName.mockReturnValue('search-v2')
    plugins.runMigrationsOnTenant.mockResolvedValue('future-migration')
    plugins.completeTenantMigrations.mockResolvedValue(0)
    const { app, injectTenant } = await buildMigrationApp(plugins)

    try {
      expect((await injectTenant()).json()).toEqual({ latestMigration: 'search-v2' })
      expect(tenant.syncMigrationsDone).toBe(false)
      expect(plugins.cacheTenantMigration).toHaveBeenCalledExactlyOnceWith(
        tenant,
        'future-migration'
      )
      // The freshness check can skip execution, without the permanent sync fast path.
      expect((await injectTenant()).json()).toEqual({ latestMigration: 'search-v2' })
      expect(tenant.syncMigrationsDone).toBe(false)
      expect(plugins.cacheTenantMigration).toHaveBeenCalledOnce()
      expect(plugins.areMigrationsUpToDate).toHaveBeenCalledTimes(2)
      expect(plugins.runMigrationsOnTenant).toHaveBeenCalledOnce()
    } finally {
      await app.close()
    }
  })

  it('does not mark a known incomplete control row synced after a zero-row completion', async () => {
    const plugins = await loadMigrationPlugins()
    const tenant = tenantRow({ migrationStatus: 'FAILED' })
    plugins.getTenantConfig.mockResolvedValue(tenant)
    plugins.areMigrationsUpToDate.mockResolvedValue(false)
    plugins.completeTenantMigrations.mockResolvedValue(0)
    const { app, injectTenant } = await buildMigrationApp(plugins)
    try {
      expect((await injectTenant()).statusCode).toBe(200)
      expect(tenant.syncMigrationsDone).toBe(false)
      expect(plugins.cacheTenantMigration).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('skips migration execution when tenant migrations are already up to date', async () => {
    const plugins = await loadMigrationPlugins()

    plugins.getTenantConfig.mockResolvedValue(tenantRow())
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

  it('observes a reloaded tenant after cache eviction when no migration runs', async () => {
    const plugins = await loadMigrationPlugins()
    const physical = 'objects-key-version-index'
    const reloaded = tenantRow({
      migrationVersion: 'revoke-grants-to-unused-operations',
      migrationStatus: 'FAILED',
    })
    // Eviction after the successful freshness check leaves no observation on the reload.
    plugins.getTenantConfig
      .mockResolvedValueOnce({
        ...reloaded,
        observedMigrationName: 'future-migration',
        observedMigration: Promise.resolve(reloaded.migrationVersion),
      })
      .mockResolvedValue(reloaded)
    plugins.areMigrationsUpToDate.mockResolvedValue(true)
    plugins.lastLocalMigrationName.mockResolvedValue(physical)
    plugins.observeTenantMigration.mockResolvedValue(physical)
    const { app, injectTenant } = await buildMigrationApp(plugins)
    try {
      const response = await injectTenant()
      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ latestMigration: physical })
      expect(plugins.observeTenantMigration).toHaveBeenCalledExactlyOnceWith('tenant-id', reloaded)
      expect(plugins.runMigrationsOnTenant).not.toHaveBeenCalled()
      expect(plugins.completeTenantMigrations).not.toHaveBeenCalled()
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

    plugins.getTenantConfig.mockImplementation(async (tenantId: string) =>
      tenantRow({
        databaseUrl: `postgres://${tenantId}`,
        databaseUrlEncrypted: `encrypted-${tenantId}`,
      })
    )
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
