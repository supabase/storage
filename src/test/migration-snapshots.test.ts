import { randomUUID } from 'node:crypto'
import { cp, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import Fastify from 'fastify'
import { Client } from 'pg'
import { vi } from 'vitest'
import { getConfig, MultitenantMigrationStrategy } from '../config'
import type { TenantConfigStorePg } from '../internal/database/tenant-store-pg'

const frozen = 'objects-key-version-index'
const future = 'future-migration'
const adminUrl = getConfig().databaseURL

type AsyncFn = (...args: never[]) => Promise<unknown>

async function loadModules(
  strategy: MultitenantMigrationStrategy,
  controlUrl: string,
  freezeAt: typeof frozen | undefined
) {
  vi.resetModules()
  const config = await import('../config')
  config.getConfig({ reload: true })
  config.mergeConfig({
    isMultitenant: true,
    multitenantDatabaseUrl: controlUrl,
    multitenantDatabasePoolUrl: undefined,
    dbMigrationStrategy: strategy,
    dbMigrationFreezeAt: freezeAt,
    pgQueueEnable: true,
    vectorStoreMigrationsEnabled: false,
  })
  const migrate = await import('../internal/database/migrations')
  const migrateFile = await import('../internal/database/migrations/migrate')
  const migrationConnection = await import('../internal/database/migrations/connection')
  const tenant = await import('../internal/database/tenant')
  const multitenant = await import('../internal/database/multitenant-pg')
  const auth = await import('../internal/auth')
  const { dbSuperUser } = await import('../http/plugins/db')
  const { default: buildAdminApp } = await import('../admin-app')
  const { setErrorHandler } = await import('../http/error-handler')
  const { StoragePgDB } = await import('../storage/database/pg')
  const { PgTenantConnection } = await import('../internal/database/pg-connection')
  const { TenantConfigStorePg } = await import('../internal/database/tenant-store-pg')
  const { RunMigrationsOnTenants } = await import('../storage/events/migrations/run-migrations')
  return {
    migrate,
    migrateFile,
    migrationConnection,
    tenant,
    multitenant,
    auth,
    dbSuperUser,
    buildAdminApp,
    setErrorHandler,
    StoragePgDB,
    PgTenantConnection,
    TenantConfigStorePg,
    RunMigrationsOnTenants,
  }
}

// Multigres does not support the disposable database fixtures used here.
describe
  .skipIf(getConfig().databaseEngine === 'multigres')
  .each(Object.values(MultitenantMigrationStrategy))('migration snapshots under %s', (strategy) => {
  const tenantId = `migration-snapshot-${randomUUID()}`
  const databases: string[] = []
  const urls: Record<string, string> = {}
  let admin: Client
  let control: Client
  let modules: Awaited<ReturnType<typeof loadModules>>

  beforeAll(async () => {
    admin = new Client({ connectionString: adminUrl })
    await admin.connect()
    for (const kind of ['control', 'frozen', 'current']) {
      const name = `migration_snapshot_${kind}_${randomUUID().replaceAll('-', '')}`
      await admin.query(`CREATE DATABASE "${name}"`)
      databases.push(name)
      const url = new URL(adminUrl)
      url.pathname = `/${name}`
      urls[kind] = url.toString()
    }
    modules = await loadModules(strategy, urls.control, frozen)
    await modules.migrate.runMultitenantMigrations()
    control = new Client({ connectionString: urls.control })
    await control.connect()
    for (const kind of ['frozen', 'current']) {
      await modules.migrate.runMigrationsOnTenant({
        databaseUrl: urls[kind],
        upToMigration: kind === 'frozen' ? frozen : undefined,
      })
      await withClient(urls[kind], (client) =>
        client.query("INSERT INTO storage.buckets (id, name) VALUES ('b', 'b')")
      )
    }
    const { encrypt } = modules.auth
    await control.query(
      `INSERT INTO tenants (id, anon_key, database_url, jwt_secret, service_key)
       VALUES ($1, $2, $3, $4, $5)`,
      [tenantId, encrypt('anon'), encrypt(urls.frozen), encrypt('secret'), encrypt('service')]
    )
  })

  afterEach(() => vi.restoreAllMocks())

  afterAll(async () => {
    await modules?.PgTenantConnection.stop()
    await modules?.multitenant.closeMultitenantPg()
    await control?.end()
    for (const name of databases) {
      await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`)
    }
    await admin?.end()
    vi.resetModules()
  })

  async function setControl(version: string | null, status: string | null, physical: string) {
    await control.query(
      'UPDATE tenants SET migrations_version = $2, migrations_status = $3, database_url = $4 WHERE id = $1',
      [tenantId, version, status, modules.auth.encrypt(urls[physical])]
    )
    modules.tenant.deleteTenantConfig(tenantId)
  }

  async function reloadModules(freezeAt: typeof frozen | undefined) {
    await modules.PgTenantConnection.stop()
    await modules.multitenant.closeMultitenantPg()
    modules = await loadModules(strategy, urls.control, freezeAt)
  }

  async function controlState() {
    const result = await control.query(
      'SELECT database_url, migrations_version, migrations_status FROM tenants WHERE id = $1',
      [tenantId]
    )
    const row = result.rows[0]
    return { ...row, database_url: modules.auth.decrypt(row.database_url) }
  }

  function job(retryCount = 0) {
    return {
      id: randomUUID(),
      retryCount,
      retryLimit: 3,
      singletonKey: `migrations_${tenantId}`,
      data: { tenantId, tenant: { ref: tenantId, host: '' }, upToMigration: frozen },
    } as Parameters<typeof modules.RunMigrationsOnTenants.handle>[0]
  }

  function updateDatabase(
    app: ReturnType<typeof modules.buildAdminApp>,
    physical: string,
    method: 'PATCH' | 'PUT' = 'PATCH'
  ) {
    return app.inject({
      method,
      url: `/tenants/${tenantId}`,
      headers: { apikey: process.env.ADMIN_API_KEYS },
      payload: {
        databaseUrl: urls[physical],
        ...(method === 'PUT'
          ? { anonKey: 'anon', jwtSecret: 'secret', serviceKey: 'service' }
          : {}),
      },
    })
  }

  function pauseRun<M extends string>(target: Record<M, AsyncFn>, method: M, failure?: Error) {
    const call: AsyncFn = target[method]
    const entered = Promise.withResolvers<unknown>()
    const resume = Promise.withResolvers<void>()
    const spy = vi
      .spyOn(target as Record<string, AsyncFn>, method)
      .mockImplementationOnce(async (...args) => {
        const result = failure ? undefined : await call(...args)
        entered.resolve(result)
        await resume.promise
        if (failure) throw failure
        return result
      })
    return { entered: entered.promise, resume: resume.resolve, spy }
  }

  async function upsert() {
    const app = Fastify()
    modules.setErrorHandler(app)
    app.decorateRequest('tenantId')
    app.addHook('onRequest', async (req) => {
      req.tenantId = tenantId
    })
    await app.register(modules.dbSuperUser, { disableHostCheck: true })
    app.post('/write', async (request) => {
      const db = new modules.StoragePgDB(request.db, {
        tenantId,
        host: 'localhost',
        latestMigration: request.latestMigration,
      })
      const object = await db.upsertObject({
        name: randomUUID(),
        bucket_id: 'b',
        version: randomUUID(),
        metadata: { mimetype: 'text/plain' },
        user_metadata: { snapshot: true },
      })
      return { latestMigration: request.latestMigration, metadata: object.user_metadata }
    })
    try {
      return await app.inject({ method: 'POST', url: '/write' })
    } finally {
      await app.close()
    }
  }

  const stubQueue = () =>
    vi.spyOn(modules.migrate.progressiveMigrations, 'addTenant').mockImplementation(() => {})

  async function expectServed(latestMigration: string) {
    const response = await upsert()
    expect(response.statusCode, response.body).toBe(200)
    expect(response.json()).toEqual({ latestMigration, metadata: { snapshot: true } })
  }

  const postMigrations = (app: ReturnType<typeof modules.buildAdminApp>) =>
    app.inject({
      method: 'POST',
      url: `/tenants/${tenantId}/migrations`,
      headers: { apikey: process.env.ADMIN_API_KEYS },
    })

  async function withClient<T>(url: string, fn: (client: Client) => Promise<T>) {
    const client = new Client({ connectionString: url })
    await client.connect()
    try {
      return await fn(client)
    } finally {
      await client.end()
    }
  }

  async function withFutureLedger(fn: () => Promise<void>) {
    const directory = await mkdtemp(join(tmpdir(), 'storage-future-migrations-'))
    try {
      await withClient(urls.current, async (client) => {
        await applyFutureLedger(client, directory)
        try {
          await fn()
        } finally {
          await client.query('DELETE FROM storage.migrations WHERE name = $1', [future])
        }
      })
    } finally {
      await rm(directory, { recursive: true, force: true })
    }
  }

  async function restoreCurrentSchema() {
    await withClient(urls.current, async (client) => {
      await client.query('DROP SCHEMA storage CASCADE')
      await modules.migrate.runMigrationsOnTenant({
        databaseUrl: urls.current,
        upToMigration: frozen,
      })
      await client.query("INSERT INTO storage.buckets (id, name) VALUES ('b', 'b')")
    })
  }

  async function applyFutureLedger(client: Client, directory: string) {
    const nextOrdinal = modules.migrate.DBMigration[modules.migrate.highestLocalMigrationName()] + 1
    await cp('migrations/tenant', directory, { recursive: true })
    for (const [id, name, sql] of [
      [nextOrdinal, future, 'SELECT 1;'],
      [nextOrdinal + 1, 'future-failure', 'SELECT 1 / 0;'],
    ] as const) {
      await writeFile(join(directory, `${String(id).padStart(4, '0')}-${name}.sql`), sql)
    }
    await expect(
      modules.migrate.migrate({
        client,
        migrationsDirectory: directory,
        migrationsTableSchema: 'storage',
        waitForLock: true,
        shouldCreateStorageSchema: true,
      })
    ).rejects.toThrow('division by zero')
  }

  it.runIf(strategy !== MultitenantMigrationStrategy.PROGRESSIVE)(
    'preserves a newer migration failure while serving the known schema',
    async () => {
      const current = modules.migrate.highestLocalMigrationName()
      await withFutureLedger(async () => {
        await expect(
          modules.migrate.readTenantMigrationVersion({ databaseUrl: urls.current })
        ).resolves.toBe(future)

        // Model the newer worker's control write after its next migration fails.
        const status =
          strategy === MultitenantMigrationStrategy.ON_REQUEST ? 'FAILED' : 'FAILED_STALE'
        await setControl(future, status, 'current')
        const expected = { migrations_version: future, migrations_status: status }
        const run = vi.spyOn(modules.migrate, 'runMigrationsOnTenant')
        stubQueue()
        if (strategy !== MultitenantMigrationStrategy.ON_REQUEST) {
          await modules.RunMigrationsOnTenants.handle(job())
          expect(await controlState()).toMatchObject(expected)
        }
        for (let attempt = 0; attempt < 2; attempt++) {
          await expectServed(current)
          expect(await controlState()).toMatchObject(expected)
        }
        expect(run).not.toHaveBeenCalled()
        if (strategy === MultitenantMigrationStrategy.FULL_FLEET) {
          const app = modules.buildAdminApp()
          try {
            const response = await postMigrations(app)
            expect(response.statusCode, response.body).toBe(200)
            expect(response.json()).toEqual({ migrated: false })
            expect(await controlState()).toMatchObject(expected)
          } finally {
            await app.close()
          }
        }

        await setControl(future, 'COMPLETED', 'current')
        run.mockClear()
        await expectServed(current)
        expect(run).not.toHaveBeenCalled()
        expect(await controlState()).toMatchObject({
          migrations_version: future,
          migrations_status: 'COMPLETED',
        })
      })
    }
  )

  it.each(
    strategy === MultitenantMigrationStrategy.FULL_FLEET
      ? []
      : [
          ['known', 'FAILED'],
          ['missing', null],
        ]
  )('revalidates an ahead ledger for a %s / %s control row and recovers after same-URL restore', async (version, status) => {
    const current = modules.migrate.highestLocalMigrationName()
    const recorded = version === 'known' ? current : null
    let now = Math.ceil(performance.now())
    const startedAt = now
    vi.spyOn(performance, 'now').mockImplementation(() => now)
    try {
      await withFutureLedger(async () => {
        await setControl(recorded, status, 'current')
        const send = vi.spyOn(modules.RunMigrationsOnTenants, 'batchSend').mockResolvedValue([])
        const run = vi.spyOn(modules.migrateFile, 'runMigrationsOnTenant')
        const read = vi.spyOn(modules.migrationConnection, 'readTenantMigrationVersion')
        const sender = modules.migrate.progressiveMigrations as unknown as {
          createJobsBatch(maxJobs: number): Promise<void>
        }

        for (let pass = 0; pass < 2; pass++) {
          now = startedAt + pass * 30_000
          for (let attempt = 0; attempt < 3; attempt++) {
            await expectServed(current)
            await expect(modules.migrate.tenantHasMigrations(tenantId, current)).resolves.toBe(true)
            await sender.createJobsBatch(200)
          }
          expect(read).toHaveBeenCalledTimes(pass + 1)
          expect(run).not.toHaveBeenCalled()
          expect(send).not.toHaveBeenCalled()
        }
        const cached = await modules.tenant.getTenantConfig(tenantId)
        expect(cached.syncMigrationsDone).toBeFalsy()
        expect(await controlState()).toMatchObject({
          migrations_version: recorded,
          migrations_status: status,
          database_url: urls.current,
        })

        await restoreCurrentSchema()
        run.mockClear()
        expect(await modules.tenant.getTenantConfig(tenantId)).toBe(cached)
        now = startedAt + 59_999
        await expect(modules.migrate.tenantHasMigrations(tenantId, current)).resolves.toBe(true)
        expect(read).toHaveBeenCalledTimes(2)
        now++
        if (version === 'missing') {
          await expect(modules.migrate.tenantHasMigrations(tenantId, current)).resolves.toBe(false)
        }
        // Known rows refresh through the request itself; missing rows through capability lookup.
        await expectServed(frozen)
        await expect(modules.migrate.tenantHasMigrations(tenantId, current)).resolves.toBe(false)
        expect(read).toHaveBeenCalledTimes(3)
        if (strategy === MultitenantMigrationStrategy.PROGRESSIVE) {
          await sender.createJobsBatch(200)
          expect(send).toHaveBeenCalledOnce()
          expect(send.mock.calls[0][0]).toHaveLength(1)
          // Model delivery of the captured batch through the real worker.
          await modules.RunMigrationsOnTenants.handle(job())
          await expectServed(frozen)
        }
        expect(run).toHaveBeenCalledOnce()
        expect(await controlState()).toMatchObject({
          migrations_version: frozen,
          migrations_status: 'COMPLETED',
          database_url: urls.current,
        })
      })
    } finally {
      await modules.migrate.runMigrationsOnTenant({ databaseUrl: urls.current })
    }
  })

  it.each(
    strategy === MultitenantMigrationStrategy.FULL_FLEET
      ? []
      : strategy === MultitenantMigrationStrategy.ON_REQUEST
        ? [[future, 'COMPLETED']]
        : [
            [future, 'COMPLETED'],
            [frozen, 'FAILED'],
          ]
  )('reuses a %s / %s observation until restore migration invalidates the config', async (version, status) => {
    await setControl(version, status, 'current')
    stubQueue()
    const read = vi.spyOn(modules.migrationConnection, 'readTenantMigrationVersion')
    const current = modules.migrate.highestLocalMigrationName()
    for (let attempt = 0; attempt < 3; attempt++) {
      await expectServed(current)
    }
    for (let attempt = 0; attempt < 5; attempt++) {
      await expect(modules.migrate.tenantHasMigrations(tenantId, current)).resolves.toBe(true)
    }
    expect(read).toHaveBeenCalledTimes(1)
    const cached = await modules.tenant.getTenantConfig(tenantId)

    const app = modules.buildAdminApp()
    try {
      await restoreCurrentSchema()
      expect(await modules.tenant.getTenantConfig(tenantId)).toBe(cached)

      const migrated = await postMigrations(app)
      expect(migrated.statusCode, migrated.body).toBe(200)
      expect(migrated.json()).toEqual({ migrated: true })
      expect(await modules.tenant.getTenantConfig(tenantId)).not.toBe(cached)

      await expectServed(frozen)
    } finally {
      await app.close()
      await modules.migrate.runMigrationsOnTenant({ databaseUrl: urls.current })
    }
  })

  it('serves an unknown completed tenant through its pool when the direct connection fails', async () => {
    await setControl(future, 'COMPLETED', 'current')
    const direct = createServer((socket) => socket.destroy())
    await new Promise<void>((resolve) => direct.listen(0, '127.0.0.1', resolve))
    const address = direct.address()
    if (!address || typeof address === 'string') throw new Error('Expected a TCP address')
    const unavailable = new URL(urls.current)
    unavailable.hostname = '127.0.0.1'
    unavailable.port = String(address.port)
    const read = vi.spyOn(modules.migrationConnection, 'readTenantMigrationVersion')
    const run = vi.spyOn(modules.migrate, 'runMigrationsOnTenant')
    const queue = stubQueue()
    try {
      await control.query(
        'UPDATE tenants SET database_url = $2, database_pool_url = $3 WHERE id = $1',
        [tenantId, modules.auth.encrypt(unavailable.toString()), modules.auth.encrypt(urls.current)]
      )
      modules.tenant.deleteTenantConfig(tenantId)
      const current = modules.migrate.highestLocalMigrationName()
      for (let attempt = 0; attempt < 2; attempt++) {
        await expectServed(current)
      }
      expect(read).toHaveBeenCalledOnce()
      await expect(modules.migrate.tenantHasMigrations(tenantId, current)).resolves.toBe(true)
      await modules.RunMigrationsOnTenants.handle(job())
      expect(run).not.toHaveBeenCalled()
      expect(queue).not.toHaveBeenCalled()
      expect(await controlState()).toMatchObject({
        migrations_version: future,
        migrations_status: 'COMPLETED',
      })
    } finally {
      await new Promise<void>((resolve) => direct.close(() => resolve()))
      await control.query('UPDATE tenants SET database_pool_url = NULL WHERE id = $1', [tenantId])
      modules.tenant.deleteTenantConfig(tenantId)
    }
  })

  const snapshotCases = [
    [future, 'FAILED', 'frozen'],
    ['current', 'FAILED', 'frozen'],
    [null, 'COMPLETED', 'current'],
    [future, 'COMPLETED', 'current'],
    [future, 'COMPLETED', 'frozen'],
    [frozen, 'COMPLETED', 'frozen'],
  ] as const
  it.each(
    // FULL_FLEET takes the PROGRESSIVE request path for every row.
    strategy === MultitenantMigrationStrategy.FULL_FLEET ? [] : snapshotCases
  )('upserts with control %s / %s and physical schema %s', async (recorded, status, physical) => {
    const current = modules.migrate.highestLocalMigrationName()
    const version = recorded === 'current' ? current : recorded
    await setControl(version, status, physical!)
    const queue = stubQueue()
    const run = vi.spyOn(modules.migrate, 'runMigrationsOnTenant')
    const incomplete = !version || status !== 'COMPLETED'

    await expectServed(physical === 'frozen' ? frozen : current)
    if (strategy !== MultitenantMigrationStrategy.ON_REQUEST) {
      expect(run).not.toHaveBeenCalled()
      expect(queue).toHaveBeenCalledTimes(
        incomplete || (version === future && physical === 'frozen') ? 1 : 0
      )
      expect(await controlState()).toMatchObject({
        migrations_version: version,
        migrations_status: status,
      })
      if (version === future && status === 'COMPLETED' && physical === 'frozen') {
        await modules.RunMigrationsOnTenants.handle(job())
        expect(await controlState()).toMatchObject({
          migrations_version: frozen,
          migrations_status: 'COMPLETED',
        })
        await expectServed(frozen)
      }
    }
  })

  if (strategy === MultitenantMigrationStrategy.PROGRESSIVE) {
    it('retries a database replacement during the ledger read', async () => {
      await setControl(frozen, 'FAILED', 'current')
      const { entered, resume } = pauseRun(
        modules.migrationConnection,
        'readTenantMigrationVersion'
      )
      const queue = stubQueue()
      const app = modules.buildAdminApp()
      const request = upsert()
      try {
        await entered
        const patch = await updateDatabase(app, 'frozen')
        expect(patch.statusCode, patch.body).toBe(204)
        resume()
        const response = await request
        expect(response.statusCode, response.body).toBe(503)
        expect(response.json()).toMatchObject({ code: 'DatabaseError' })
        expect(queue).toHaveBeenCalledWith(tenantId)
        expect(await controlState()).toEqual({
          database_url: urls.frozen,
          migrations_version: frozen,
          migrations_status: 'COMPLETED',
        })
        await expectServed(frozen)
      } finally {
        resume()
        await request
        await app.close()
      }
    })

    it.each([
      [frozen, 'FAILED', 200],
      [future, 'FAILED', 503],
      [future, 'COMPLETED', 500],
    ] as const)('retries a failed ledger read with %s / %s without evicting the tenant config', async (version, status, initialStatus) => {
      await setControl(version, status, 'frozen')
      const tenant = await modules.tenant.getTenantConfig(tenantId)
      const queue = stubQueue()
      const read = vi
        .spyOn(modules.migrationConnection, 'readTenantMigrationVersion')
        .mockRejectedValueOnce(new Error('ledger unavailable'))
      const run = vi.spyOn(modules.migrate, 'runMigrationsOnTenant')
      const write = vi.spyOn(modules.StoragePgDB.prototype, 'upsertObject')

      const first = await upsert()
      expect(first.statusCode, first.body).toBe(initialStatus)
      expect(first.json()).toMatchObject(
        initialStatus === 200
          ? { latestMigration: frozen, metadata: { snapshot: true } }
          : { statusCode: String(initialStatus), code: 'DatabaseError' }
      )
      if (initialStatus === 500) {
        // Completed fallback trusts the control row until the ledger is readable again.
        expect(first.json().message).toContain('42P10')
      }
      expect(write).toHaveBeenCalledTimes(initialStatus === 503 ? 0 : 1)
      expect(read).toHaveBeenCalledTimes(1)
      expect(queue).toHaveBeenCalledTimes(status === 'COMPLETED' ? 0 : 1)

      // Model the delayed clear of the failed observation.
      tenant.observedMigration = undefined
      for (let request = 0; request < 2; request++) {
        await expectServed(frozen)
      }
      expect(read).toHaveBeenCalledTimes(2)
      expect(queue).toHaveBeenCalledTimes(status === 'COMPLETED' ? 2 : 3)
      expect(run).not.toHaveBeenCalled()
      expect(await modules.tenant.getTenantConfig(tenantId)).toBe(tenant)
      expect(await controlState()).toMatchObject({
        migrations_version: version,
        migrations_status: status,
      })
    })

    it('keeps the recorded version when a same-URL PUT fails to migrate', async () => {
      await setControl(frozen, 'COMPLETED', 'frozen')
      vi.spyOn(modules.migrate, 'runMigrationsOnTenant').mockRejectedValueOnce(
        new Error('migration failed')
      )
      const queue = stubQueue()
      const app = modules.buildAdminApp()
      try {
        const response = await updateDatabase(app, 'frozen', 'PUT')
        expect(response.statusCode, response.body).toBe(204)
        expect(queue).toHaveBeenCalledWith(tenantId)
        expect(await controlState()).toEqual({
          database_url: urls.frozen,
          migrations_version: frozen,
          migrations_status: 'FAILED',
        })
        const read = vi
          .spyOn(modules.migrationConnection, 'readTenantMigrationVersion')
          .mockRejectedValueOnce(new Error('ledger unavailable'))
        await expectServed(frozen)
        expect(read).toHaveBeenCalledOnce()
      } finally {
        await app.close()
      }
    })
  }

  it('retries a failed ahead refresh before syncing a completed control row', async () => {
    await reloadModules(undefined)
    const current = modules.migrate.highestLocalMigrationName()
    await setControl(current, 'COMPLETED', 'current')
    const cached = await modules.tenant.getTenantConfig(tenantId)
    let now = Math.ceil(performance.now())
    vi.spyOn(performance, 'now').mockImplementation(() => now)
    try {
      await withFutureLedger(async () => {
        await modules.migrate.observeTenantMigration(tenantId, cached)
        expect(cached.observedMigrationName).toBe(future)
        const send = vi
          .spyOn(modules.RunMigrationsOnTenants, 'batchSend')
          .mockResolvedValue(undefined)
        const read = vi.spyOn(modules.migrationConnection, 'readTenantMigrationVersion')
        const run = vi.spyOn(modules.migrateFile, 'runMigrationsOnTenant')
        now += 30_000
        await withClient(urls.current, (client) => client.query('DROP SCHEMA storage CASCADE'))
        try {
          const unavailable = await upsert()
          expect(unavailable.json().code).toBe('DatabaseSchemaMismatch')
          await expect(read.mock.results[0].value).rejects.toMatchObject({ code: '42P01' })
          expect(cached.observedMigrationName).toBe(future)
          expect(cached.observedMigrationExpiresAt).toBe(now + 5_000)
          expect(cached.syncMigrationsDone).toBeFalsy()
          expect(run).not.toHaveBeenCalled()
        } finally {
          await modules.migrate.runMigrationsOnTenant({
            databaseUrl: urls.current,
            upToMigration: frozen,
          })
          await withClient(urls.current, (client) =>
            client.query("INSERT INTO storage.buckets (id, name) VALUES ('b', 'b')")
          )
        }
        expect(await modules.tenant.getTenantConfig(tenantId)).toBe(cached)
        read.mockClear()
        run.mockClear()
        now += 4_999
        const fallback = await upsert()
        expect(fallback.statusCode, fallback.body).toBe(500)
        expect(fallback.json().message).toContain('42P10')
        expect(read).not.toHaveBeenCalled()
        expect(cached.syncMigrationsDone).toBeFalsy()
        now += 1
        await expectServed(strategy === MultitenantMigrationStrategy.ON_REQUEST ? current : frozen)
        expect(read).toHaveBeenCalledOnce()
        if (strategy !== MultitenantMigrationStrategy.ON_REQUEST) {
          expect(cached.syncMigrationsDone).toBeFalsy()
          expect(run).not.toHaveBeenCalled()
          await modules.migrate.progressiveMigrations.drain()
          expect(send).toHaveBeenCalledOnce()
          const event = send.mock.calls[0][0][0]
          if (!(event instanceof modules.RunMigrationsOnTenants)) {
            throw new Error('Expected a tenant migration event')
          }
          expect(event.payload.upToMigration).toBeUndefined()
          await modules.RunMigrationsOnTenants.handle({ ...job(), data: event.payload })
        }
        expect(run).toHaveBeenCalledOnce()
        await expect(
          modules.migrate.readTenantMigrationVersion({ databaseUrl: urls.current })
        ).resolves.toBe(current)
        expect(await controlState()).toMatchObject({
          migrations_version: current,
          migrations_status: 'COMPLETED',
          database_url: urls.current,
        })
        modules.tenant.deleteTenantConfig(tenantId)
        await expectServed(current)
        expect(run).toHaveBeenCalledOnce()
      })
    } finally {
      await modules.migrate.runMigrationsOnTenant({ databaseUrl: urls.current })
      await reloadModules(frozen)
    }
  })

  if (strategy !== MultitenantMigrationStrategy.ON_REQUEST) {
    it.each([
      'request',
      'capability',
    ] as const)('serves a restored floor and repairs it through the worker after %s refresh', async (refresh) => {
      await reloadModules(undefined)
      const current = modules.migrate.highestLocalMigrationName()
      await setControl(current, 'COMPLETED', 'current')
      const cached = await modules.tenant.getTenantConfig(tenantId)
      let now = Math.ceil(performance.now())
      vi.spyOn(performance, 'now').mockImplementation(() => now)
      try {
        await withFutureLedger(async () => {
          await modules.migrate.observeTenantMigration(tenantId, cached)
          expect(cached.observedMigrationName).toBe(future)
          expect(cached.observedMigrationExpiresAt).toBe(now + 30_000)
          await restoreCurrentSchema()
          expect(await modules.tenant.getTenantConfig(tenantId)).toBe(cached)
          now += 30_000
          if (refresh === 'capability') {
            await expect(modules.migrate.tenantHasMigrations(tenantId, current)).resolves.toBe(
              false
            )
          }
          const send = vi
            .spyOn(modules.RunMigrationsOnTenants, 'batchSend')
            .mockResolvedValue(undefined)
          const run = vi.spyOn(modules.migrateFile, 'runMigrationsOnTenant')
          await expectServed(frozen)
          expect(cached.observedMigrationName).toBe(frozen)
          expect(cached.syncMigrationsDone).toBeFalsy()
          expect(run).not.toHaveBeenCalled()
          await modules.migrate.progressiveMigrations.drain()
          expect(send).toHaveBeenCalledOnce()
          expect(send.mock.calls[0][0]).toHaveLength(1)
          const event = send.mock.calls[0][0][0]
          if (!(event instanceof modules.RunMigrationsOnTenants)) {
            throw new Error('Expected a tenant migration event')
          }
          expect(event.payload.upToMigration).toBeUndefined()
          await modules.RunMigrationsOnTenants.handle({ ...job(), data: event.payload })
          expect(run).toHaveBeenCalledOnce()
          await expect(
            modules.migrate.readTenantMigrationVersion({ databaseUrl: urls.current })
          ).resolves.toBe(current)
          expect(await modules.tenant.getTenantConfig(tenantId)).not.toBe(cached)
          await expectServed(current)
          modules.tenant.deleteTenantConfig(tenantId)
          await expectServed(current)
          await expect(modules.migrate.tenantHasMigrations(tenantId, current)).resolves.toBe(true)
          expect(await controlState()).toMatchObject({
            migrations_version: current,
            migrations_status: 'COMPLETED',
            database_url: urls.current,
          })
        })
      } finally {
        await modules.migrate.runMigrationsOnTenant({ databaseUrl: urls.current })
        await reloadModules(frozen)
      }
    })
  }

  if (strategy === MultitenantMigrationStrategy.ON_REQUEST) {
    it.each([
      { target: 'frozen', refresh: 'request' },
      { target: 'frozen', refresh: 'capability' },
      { target: 'latest', refresh: 'request' },
      { target: 'latest', refresh: 'capability' },
    ] as const)('serves the restored ledger after ahead expiry with $target / $refresh', async ({
      target,
      refresh,
    }) => {
      if (target === 'latest') await reloadModules(undefined)
      const current = modules.migrate.highestLocalMigrationName()
      await setControl(current, 'FAILED', 'current')
      const initial = await modules.tenant.getTenantConfig(tenantId)
      let now = Math.ceil(performance.now())
      vi.spyOn(performance, 'now').mockImplementation(() => now)
      const paused = pauseRun(modules.migrationConnection, 'readTenantMigrationVersion')
      const run = vi.spyOn(modules.migrateFile, 'runMigrationsOnTenant')
      const warm = upsert()
      try {
        await expect(paused.entered).resolves.toBe(current)
        await withFutureLedger(async () => {
          // A concurrent worker completes the local head it read before the ledger advanced.
          await expect(
            modules.migrate.completeTenantMigrations(tenantId, {
              expectedMigrationVersion: current,
              expectedDatabaseUrl: initial.databaseUrlEncrypted,
              migration: current,
            })
          ).resolves.toBe(1)
          modules.tenant.deleteTenantConfig(tenantId)
          paused.resume()
          const warmed = await warm
          expect(warmed.statusCode, warmed.body).toBe(200)
          const cached = await modules.tenant.getTenantConfig(tenantId)
          expect(cached.migrationVersion).toBe(current)
          expect(cached.migrationStatus).toBe('COMPLETED')
          expect(cached.observedMigrationName).toBe(future)
          expect(cached.syncMigrationsDone).toBeFalsy()
          expect(cached.observedMigrationExpiresAt).toBe(now + 30_000)

          await restoreCurrentSchema()
          expect(await modules.tenant.getTenantConfig(tenantId)).toBe(cached)
          run.mockClear()
          now += 30_000
          if (refresh === 'capability') {
            await expect(modules.migrate.tenantHasMigrations(tenantId, current)).resolves.toBe(
              false
            )
          }
          const expected = target === 'latest' ? current : frozen
          await expectServed(expected)
          await expectServed(expected)
          expect(run).toHaveBeenCalledTimes(target === 'latest' ? 1 : 0)
          await expect(
            modules.migrate.readTenantMigrationVersion({ databaseUrl: urls.current })
          ).resolves.toBe(expected)
          await expect(modules.migrate.tenantHasMigrations(tenantId, current)).resolves.toBe(
            target === 'latest'
          )
          expect(await controlState()).toMatchObject({
            migrations_version: current,
            migrations_status: 'COMPLETED',
            database_url: urls.current,
          })
          if (target === 'latest') {
            modules.tenant.deleteTenantConfig(tenantId)
            await expectServed(current)
            expect(run).toHaveBeenCalledOnce()
          }
        })
      } finally {
        paused.resume()
        await warm
        await modules.migrate.runMigrationsOnTenant({ databaseUrl: urls.current })
        if (target === 'latest') await reloadModules(frozen)
      }
    })

    it('retries against the new database when its URL changes during an on-request migration', async () => {
      await setControl(frozen, 'FAILED', 'current')
      const connection = vi.spyOn(
        await import('../internal/database/client'),
        'getPostgresConnection'
      )
      const { entered, resume, spy: runSpy } = pauseRun(modules.migrate, 'runMigrationsOnTenant')
      runSpy.mockRejectedValueOnce(new Error('new database temporarily unavailable'))
      stubQueue()
      const app = modules.buildAdminApp()
      const request = upsert()
      try {
        await entered
        const patch = await updateDatabase(app, 'frozen')
        expect(patch.statusCode, patch.body).toBe(204)
        resume()
        const response = await request
        expect(response.statusCode, response.body).toBe(503)
        expect(response.json()).toMatchObject({ code: 'DatabaseError' })
        expect(connection).not.toHaveBeenCalled()
        expect((await modules.tenant.getTenantConfig(tenantId)).syncMigrationsDone).toBeFalsy()
        expect(await controlState()).toEqual({
          database_url: urls.frozen,
          migrations_version: null,
          migrations_status: 'FAILED',
        })

        await expectServed(frozen)
        expect(connection).toHaveBeenCalledOnce()
        expect(runSpy).toHaveBeenLastCalledWith(
          expect.objectContaining({ databaseUrl: urls.frozen })
        )
      } finally {
        resume()
        await request
        await app.close()
      }
    })

    it('retries a same-URL PATCH during a shared flight', async () => {
      const current = modules.migrate.highestLocalMigrationName()
      await setControl(frozen, 'FAILED', 'current')
      const { entered, resume } = pauseRun(modules.migrate, 'runMigrationsOnTenant')
      const getTenant = vi.spyOn(modules.tenant, 'getTenantConfig')
      const app = modules.buildAdminApp()
      const first = upsert()
      let second: ReturnType<typeof upsert> | undefined
      try {
        expect(await entered).toBe(current)
        const calls = getTenant.mock.calls.length
        second = upsert()
        await vi.waitFor(() =>
          expect(getTenant.mock.calls.length).toBeGreaterThanOrEqual(calls + 1)
        )
        const update = await updateDatabase(app, 'current')
        expect(update.statusCode, update.body).toBe(204)
        resume()
        for (const response of await Promise.all([first, second])) {
          expect(response.statusCode, response.body).toBe(503)
          expect(response.json()).toMatchObject({ code: 'DatabaseError' })
        }
        expect(await controlState()).toEqual({
          database_url: urls.current,
          migrations_version: current,
          migrations_status: 'COMPLETED',
        })
        await expectServed(current)
      } finally {
        resume()
        await first
        await second
        await app.close()
      }
    })

    it.each([
      'after unlock',
      'before completion',
      'after completion',
    ] as const)('keeps the observed schema when reset runs %s', async (timing) => {
      const current = modules.migrate.highestLocalMigrationName()
      // Reset only rewinds the ledger. Replay to restore it between cases.
      await modules.migrate.runMigrationsOnTenant({ databaseUrl: urls.current })
      await setControl(current, 'FAILED', 'current')
      let resetRan = false
      async function reset() {
        resetRan = await modules.migrate.resetMigration({
          tenantId,
          databaseUrl: urls.current,
          untilMigration: frozen,
        })
        modules.tenant.deleteTenantConfig(tenantId)
      }

      if (timing === 'after unlock') {
        const query = Client.prototype.query
        let armed = true
        vi.spyOn(Client.prototype, 'query').mockImplementation(function (
          this: Client,
          ...args: Parameters<Client['query']>
        ) {
          const result = Reflect.apply(query, this, args)
          if (armed && args[0] === 'SELECT pg_advisory_unlock(-8525285245963000605);') {
            armed = false
            // Let a second connection acquire the real lock before this run resumes.
            return Promise.resolve(result).then(async (value) => {
              await reset()
              return value
            })
          }
          return result
        })
      } else {
        const complete = modules.TenantConfigStorePg.prototype.completeMigrations
        vi.spyOn(modules.TenantConfigStorePg.prototype, 'completeMigrations').mockImplementation(
          async function (this: TenantConfigStorePg, ...args) {
            if (timing === 'before completion') await reset()
            const updated = await complete.apply(this, args)
            expect(updated).toBe(timing === 'before completion' ? 0 : 1)
            if (timing === 'after completion') await reset()
            return updated
          }
        )
      }

      const response = await upsert()

      expect(resetRan).toBe(true)
      expect(response.statusCode, response.body).toBe(200)
      expect(response.json()).toEqual({
        latestMigration: current,
        metadata: { snapshot: true },
      })
      await expect(
        modules.migrate.readTenantMigrationVersion({ databaseUrl: urls.current })
      ).resolves.toBe(frozen)
      expect(await controlState()).toMatchObject({
        migrations_version: frozen,
        migrations_status: 'COMPLETED',
      })
      await expectServed(current)
      await expect(modules.migrate.tenantHasMigrations(tenantId, current)).resolves.toBe(true)
    })
  }

  if (strategy === MultitenantMigrationStrategy.FULL_FLEET) {
    it.each([
      ['admin', 'newer'],
      ['admin', 'same'],
      ['worker', 'same'],
    ] as const)('preserves completion after late %s failure (%s version, FAILED status)', async (caller, version) => {
      const physical = version === 'same' ? 'frozen' : 'current'
      const completed = version === 'same' ? frozen : modules.migrate.highestLocalMigrationName()
      await setControl(version === 'same' ? frozen : null, 'FAILED', physical)
      const failure = new Error(`old ${caller} run failed`)
      const { entered, resume } = pauseRun(modules.migrate, 'runMigrationsOnTenant', failure)
      const queue = stubQueue()
      vi.spyOn(modules.RunMigrationsOnTenants, 'deleteIfActiveExists').mockResolvedValue(undefined)
      const app = caller === 'admin' ? modules.buildAdminApp() : undefined
      const late = app
        ? updateDatabase(app, physical)
        : modules.RunMigrationsOnTenants.handle(job(3)).catch((error) => error)
      try {
        await entered
        // A real second run completes, including a no-op at the same ledger version.
        await modules.RunMigrationsOnTenants.handle(job())
        resume()
        if (app) {
          const response = await late
          expect(response.statusCode, response.body).toBe(204)
          expect(queue).toHaveBeenCalledWith(tenantId)
        } else {
          expect(await late).toBe(failure)
        }
        expect(await controlState()).toEqual({
          database_url: urls[physical],
          migrations_version: completed,
          migrations_status: 'COMPLETED',
        })
      } finally {
        resume()
        await late
        await app?.close()
      }
    })

    it('keeps the returned snapshot tied to its own update when another update wins before migration', async () => {
      await setControl(frozen, 'FAILED', 'current')
      const update = modules.TenantConfigStorePg.prototype.update
      vi.spyOn(modules.TenantConfigStorePg.prototype, 'update').mockImplementationOnce(
        async function (this: TenantConfigStorePg, ...args) {
          const snapshot = await update.apply(this, args)
          await setControl(frozen, 'PENDING', 'frozen')
          return snapshot
        }
      )
      const run = vi.spyOn(modules.migrate, 'runMigrationsOnTenant')
      const queue = stubQueue()
      const app = modules.buildAdminApp()
      try {
        const response = await updateDatabase(app, 'current')
        expect(response.statusCode, response.body).toBe(204)
        expect(queue).toHaveBeenCalledWith(tenantId)
        expect(run).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({ databaseUrl: urls.current })
        )
        expect(await controlState()).toEqual({
          database_url: urls.frozen,
          migrations_version: frozen,
          migrations_status: 'PENDING',
        })
      } finally {
        await app.close()
      }
    })

    it.each([
      'PATCH',
      'PUT',
    ] as const)('%s replaces a stored database URL that cannot be decrypted', async (method) => {
      await setControl(frozen, 'COMPLETED', 'frozen')
      await control.query('UPDATE tenants SET database_url = $2 WHERE id = $1', [
        tenantId,
        Buffer.concat([Buffer.from('Salted__'), Buffer.alloc(40, 7)]).toString('base64'),
      ])
      stubQueue()
      const app = modules.buildAdminApp()
      try {
        const response = await updateDatabase(app, 'frozen', method)
        expect(response.statusCode, response.body).toBe(204)
        expect(await controlState()).toMatchObject({ database_url: urls.frozen })
      } finally {
        await app.close()
      }
    })

    it('recovers an unknown null-status tenant through a frozen fleet job', async () => {
      await setControl(future, null, 'frozen')
      const Worker = modules.RunMigrationsOnTenants
      const send = vi.spyOn(Worker, 'batchSend').mockResolvedValue(undefined)

      await modules.migrate.runMigrationsOnAllTenants({ signal: new AbortController().signal })

      const job = send.mock.calls
        .flatMap(([jobs]) => jobs)
        .find((job) => job.payload.tenant.ref === tenantId)
      expect(job).toBeDefined()
      expect(job!.payload).toMatchObject({ upToMigration: frozen })
      await Worker.handle({
        id: randomUUID(),
        data: job!.payload,
        retryCount: 0,
        retryLimit: 3,
      } as Parameters<typeof Worker.handle>[0])
      expect(await controlState()).toMatchObject({
        migrations_version: frozen,
        migrations_status: 'COMPLETED',
      })
      await expectServed(frozen)
    })

    it('recovers a missing ledger for an unknown failed snapshot', async () => {
      await setControl(future, 'FAILED', 'current')
      // Model a same-URL restore to a database that never had the storage schema.
      await withClient(urls.current, (client) => client.query('DROP SCHEMA storage CASCADE'))
      await expect(
        modules.migrate.readTenantMigrationVersion({ databaseUrl: urls.current })
      ).rejects.toThrow()

      const work = job()
      work.data.upToMigration = undefined
      await modules.RunMigrationsOnTenants.handle(work)

      const current = modules.migrate.highestLocalMigrationName()
      expect(await controlState()).toMatchObject({
        migrations_version: current,
        migrations_status: 'COMPLETED',
      })
      await withClient(urls.current, (client) =>
        client.query("INSERT INTO storage.buckets (id, name) VALUES ('b', 'b')")
      )
      await expectServed(current)
    })
  }
})
