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
  // One digit shorter than the update below, so a string comparison would reject it.
  updated_at_us: '999999999999999',
}
const newerRow = {
  ...row,
  migrations_version: 'storage-schema',
  migrations_status: 'COMPLETED',
  updated_at_us: '1000000000000000',
}
const newerUpdate = {
  tenantId,
  type: 'update',
  migrationVersion: 'storage-schema',
  migrationStatus: 'COMPLETED',
  updatedAtUs: '1000000000000000',
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

  it.each([
    [
      'a newer value',
      row,
      newerUpdate,
      {
        migrationVersion: 'storage-schema',
        migrationStatus: 'COMPLETED',
        updatedAtUs: '1000000000000000',
        syncMigrationsDone: false,
      },
    ],
    [
      'a known version over an ahead one',
      { ...row, migrations_version: 'future-migration' },
      newerUpdate,
      { migrationVersion: 'storage-schema', migrationVersionAhead: false },
    ],
    [
      'a null version and status',
      row,
      { ...newerUpdate, migrationVersion: null, migrationStatus: null },
      { migrationVersion: null, migrationStatus: null },
    ],
  ])(
    'installs %s on the cached config without a database read',
    async (_, cached, message, expected) => {
      const read = vi.spyOn(TenantConfigStorePg.prototype, 'findById').mockResolvedValue(cached)
      const config = await getTenantConfig(tenantId)
      config.syncMigrationsDone = true

      notify(message)

      expect(await getTenantConfig(tenantId)).toBe(config)
      expect(config).toMatchObject(expected)
      expect(read).toHaveBeenCalledTimes(1)
    }
  )

  it.each(['999999999999999', '999999999999998'])(
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
        updatedAtUs: '999999999999999',
        syncMigrationsDone: true,
      })
      expect(read).toHaveBeenCalledTimes(1)
    }
  )

  it.each([
    ['a cached row without a timestamp', { ...row, updated_at_us: null }, newerUpdate],
    ['an invalid timestamp', row, { ...newerUpdate, updatedAtUs: 'not-a-timestamp' }],
    ['a wrongly typed version', row, { ...newerUpdate, migrationVersion: 1 }],
    ['a wrongly typed status', row, { ...newerUpdate, migrationStatus: 1 }],
    ['an unknown migration version', row, { ...newerUpdate, migrationVersion: 'future-migration' }],
    ['an invalidate message', row, { type: 'invalidate', tenantId }],
    ['a non-update message', row, { ...newerUpdate, type: 'delete' }],
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
      updatedAtUs: '1000000000000000',
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
