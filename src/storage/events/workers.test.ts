import { Queue } from '@internal/queue'
import { vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  config: {
    isMultitenant: false,
    storageLifecycleEnabled: true,
  },
}))

vi.mock('../../config', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../config')>()
  return { ...actual, getConfig: () => ({ ...actual.getConfig(), ...mocks.config }) }
})
vi.mock('./cdn/purge-cdn-cache', () => ({ PurgeCdnCache: class {} }))
vi.mock('./iceberg/delete-iceberg-resources', () => ({ DeleteIcebergResources: class {} }))
vi.mock('./iceberg/reclaim-shard-slots', () => ({ ReclaimIcebergShardSlots: class {} }))
vi.mock('./iceberg/reconcile-catalog', () => ({ ReconcileIcebergCatalog: class {} }))
vi.mock('./jwks/jwks-create-signing-secret', () => ({ JwksCreateSigningSecret: class {} }))
vi.mock('./jwks/jwks-roll-url-signing-key', () => ({ JwksRollUrlSigningKey: class {} }))
vi.mock('./lifecycle/webhook', () => ({ Webhook: class {} }))
vi.mock('./migrations/reset-migrations', () => ({ ResetMigrationsOnTenant: class {} }))
vi.mock('./migrations/run-migrations', () => ({ RunMigrationsOnTenants: class {} }))
vi.mock('./objects/backup-object', () => ({ BackupObjectEvent: class {} }))
vi.mock('./objects/object-admin-delete', () => ({ ObjectAdminDelete: class {} }))
vi.mock('./objects/object-admin-delete-all-before', () => ({
  ObjectAdminDeleteAllBefore: class {},
}))
vi.mock('./pgboss/move-jobs', () => ({ MoveJobs: class {} }))
vi.mock('./upgrades/sync-catalog-ids', () => ({ SyncCatalogIds: class {} }))
vi.mock('./lifecycle-rules', () => ({
  LifecycleBucketShardRun: class {},
  LifecycleTenantRun: class {},
}))

import { registerWorkers } from './workers'

describe('lifecycle worker registration', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    mocks.config.isMultitenant = false
    mocks.config.storageLifecycleEnabled = true
  })

  it.each([
    { isMultitenant: false, flag: true, registered: true },
    { isMultitenant: false, flag: false, registered: false },
    { isMultitenant: true, flag: false, registered: true },
    { isMultitenant: true, flag: true, registered: true },
  ])('registerWorkers with multitenant=$isMultitenant lifecycle flag=$flag', ({
    isMultitenant,
    flag,
    registered,
  }) => {
    mocks.config.isMultitenant = isMultitenant
    mocks.config.storageLifecycleEnabled = flag
    const register = vi.spyOn(Queue, 'register').mockImplementation(() => {})

    registerWorkers()

    const names = register.mock.calls.map(([event]) => (event as { name: string }).name)
    expect(names.includes('LifecycleTenantRun')).toBe(registered)
    expect(names.includes('LifecycleBucketShardRun')).toBe(registered)
  })
})
