import { encrypt } from '@internal/auth'
import type { PubSubAdapter } from '@internal/pubsub'
import { vi } from 'vitest'
import { deleteTenantConfig, getTenantConfig, listenForTenantUpdate } from './tenant'
import { TenantConfigStorePg } from './tenant-store-pg'

const tenantId = 'migration-cache-test'
const row = {
  id: tenantId,
  anon_key: encrypt('anon'),
  database_url: encrypt('database'),
  jwt_secret: encrypt('secret'),
  service_key: encrypt('service'),
  migrations_version: 'initialmigration',
  migrations_status: 'FAILED',
  updated_at_us: '1700000000000000',
}
const newerRow = {
  ...row,
  migrations_version: 'storage-schema',
  migrations_status: 'COMPLETED',
  updated_at_us: '1700000000000001',
}
const newerUpdate = {
  tenantId,
  type: 'update',
  migrationVersion: 'storage-schema',
  migrationStatus: 'COMPLETED',
  updatedAtUs: '1700000000000001',
}

describe('tenant migration notifications', () => {
  const listeners = new Map<string, (payload: unknown) => void>()

  beforeEach(async () => {
    deleteTenantConfig(tenantId)
    listeners.clear()
    await listenForTenantUpdate({
      subscribe: async (channel: string, callback: (payload: unknown) => void) => {
        listeners.set(channel, callback)
      },
    } as PubSubAdapter)
  })

  afterEach(() => deleteTenantConfig(tenantId))

  const notify = (message: unknown) => {
    const listener = listeners.get('tenants_migrations_update')
    expect(listener).toBeDefined()
    listener!(message)
  }

  it('installs a newer value on the cached config without a database read', async () => {
    const read = vi.spyOn(TenantConfigStorePg.prototype, 'findById').mockResolvedValue(row)
    const config = await getTenantConfig(tenantId)
    config.syncMigrationsDone = true

    notify(newerUpdate)

    expect(await getTenantConfig(tenantId)).toBe(config)
    expect(config).toMatchObject({
      migrationVersion: 'storage-schema',
      migrationStatus: 'COMPLETED',
      updatedAtUs: '1700000000000001',
      syncMigrationsDone: false,
    })
    expect(read).toHaveBeenCalledTimes(1)
  })

  it('clears an ahead version when a known version is installed', async () => {
    const read = vi
      .spyOn(TenantConfigStorePg.prototype, 'findById')
      .mockResolvedValue({ ...row, migrations_version: 'future-migration' })
    const config = await getTenantConfig(tenantId)
    expect(config.migrationVersionAhead).toBe(true)

    notify(newerUpdate)

    expect(await getTenantConfig(tenantId)).toBe(config)
    expect(config).toMatchObject({
      migrationVersion: 'storage-schema',
      migrationVersionAhead: false,
    })
    expect(read).toHaveBeenCalledTimes(1)
  })

  it('installs a failed state without a migration version', async () => {
    const read = vi.spyOn(TenantConfigStorePg.prototype, 'findById').mockResolvedValue(row)
    const config = await getTenantConfig(tenantId)

    notify({ ...newerUpdate, migrationVersion: null, migrationStatus: 'FAILED_STALE' })

    expect(await getTenantConfig(tenantId)).toBe(config)
    expect(config).toMatchObject({ migrationVersion: null, migrationStatus: 'FAILED_STALE' })
    expect(read).toHaveBeenCalledTimes(1)
  })

  it('installs a null migration status', async () => {
    const read = vi.spyOn(TenantConfigStorePg.prototype, 'findById').mockResolvedValue(row)
    const config = await getTenantConfig(tenantId)

    notify({ ...newerUpdate, migrationStatus: null })

    expect(await getTenantConfig(tenantId)).toBe(config)
    expect(config).toMatchObject({ migrationVersion: 'storage-schema', migrationStatus: null })
    expect(read).toHaveBeenCalledTimes(1)
  })

  it('compares timestamps as integers', async () => {
    vi.spyOn(TenantConfigStorePg.prototype, 'findById').mockResolvedValue({
      ...row,
      updated_at_us: '999999999999999',
    })
    const config = await getTenantConfig(tenantId)

    notify({ ...newerUpdate, updatedAtUs: '1000000000000000' })

    expect(config).toMatchObject({
      migrationVersion: 'storage-schema',
      updatedAtUs: '1000000000000000',
    })
  })

  it.each(['1700000000000000', '1699999999999999'])(
    'ignores a notification with timestamp %s',
    async (updatedAtUs) => {
      const read = vi.spyOn(TenantConfigStorePg.prototype, 'findById').mockResolvedValue(row)
      const config = await getTenantConfig(tenantId)
      config.syncMigrationsDone = true

      notify({ ...newerUpdate, updatedAtUs })

      expect(await getTenantConfig(tenantId)).toBe(config)
      expect(config).toMatchObject({
        migrationVersion: 'initialmigration',
        migrationStatus: 'FAILED',
        updatedAtUs: '1700000000000000',
        syncMigrationsDone: true,
      })
      expect(read).toHaveBeenCalledTimes(1)
    }
  )

  it.each([
    ['a cached row without a timestamp', { ...row, updated_at_us: null }, newerUpdate],
    ['a missing timestamp', row, { ...newerUpdate, updatedAtUs: undefined }],
    ['an invalid timestamp', row, { ...newerUpdate, updatedAtUs: 'not-a-timestamp' }],
    ['a wrongly typed version', row, { ...newerUpdate, migrationVersion: 1 }],
    ['a wrongly typed status', row, { ...newerUpdate, migrationStatus: 1 }],
    ['an unknown migration version', row, { ...newerUpdate, migrationVersion: 'future-migration' }],
    ['an invalidate message', row, { type: 'invalidate', tenantId }],
    ['a delete message', row, { type: 'delete', tenantId }],
  ])('reloads the config for %s', async (_, cached, message) => {
    const read = vi.spyOn(TenantConfigStorePg.prototype, 'findById').mockResolvedValue(cached)
    const config = await getTenantConfig(tenantId)

    notify(message)

    expect(await getTenantConfig(tenantId)).not.toBe(config)
    expect(read).toHaveBeenCalledTimes(2)
  })

  it('does not read the database for an uncached tenant', () => {
    const read = vi.spyOn(TenantConfigStorePg.prototype, 'findById')
    notify(newerUpdate)
    expect(read).not.toHaveBeenCalled()
  })

  it('does not publish a config read that started before a notification', async () => {
    const pending = Promise.withResolvers<Awaited<ReturnType<TenantConfigStorePg['findById']>>>()
    const read = vi
      .spyOn(TenantConfigStorePg.prototype, 'findById')
      .mockReturnValueOnce(pending.promise)
      .mockResolvedValue(newerRow)

    const config = getTenantConfig(tenantId)
    notify(newerUpdate)
    pending.resolve(row)

    expect(await config).toMatchObject({
      migrationVersion: 'storage-schema',
      migrationStatus: 'COMPLETED',
      updatedAtUs: '1700000000000001',
    })
    expect(read).toHaveBeenCalledTimes(2)
  })

  it.each(['tenant-id-string', { tenantId: 1 }, null])(
    'ignores a message without a string tenant id: %j',
    async (message) => {
      const read = vi.spyOn(TenantConfigStorePg.prototype, 'findById').mockResolvedValue(row)
      const config = await getTenantConfig(tenantId)

      expect(() => notify(message)).not.toThrow()

      expect(await getTenantConfig(tenantId)).toBe(config)
      expect(read).toHaveBeenCalledTimes(1)
    }
  )
})
