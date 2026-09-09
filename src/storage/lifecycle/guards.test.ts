import * as tenants from '@internal/database'
import { describe, expect, it, vi } from 'vitest'
import * as config from '../../config'
import {
  assertLifecycleSchemaReady,
  isLifecycleEnabledForTenant,
  isLifecycleExecutionEnabled,
} from './guards'

describe('isLifecycleEnabledForTenant', () => {
  it.each([
    { multitenant: false, lifecycle: true, versioning: false, feature: false, allowed: true },
    { multitenant: false, lifecycle: false, versioning: true, feature: true, allowed: false },
    { multitenant: true, lifecycle: false, versioning: false, feature: true, allowed: true },
    { multitenant: true, lifecycle: true, versioning: true, feature: false, allowed: false },
  ])('allowed=$allowed with multitenant=$multitenant lifecycle=$lifecycle versioning=$versioning feature=$feature', async ({
    multitenant,
    lifecycle,
    versioning,
    feature,
    allowed,
  }) => {
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      isMultitenant: multitenant,
      storageLifecycleEnabled: lifecycle,
      storageVersioningEnabled: versioning,
    })
    vi.spyOn(tenants, 'tenantHasFeature').mockResolvedValue(feature)

    await expect(isLifecycleEnabledForTenant('tenant-a')).resolves.toBe(allowed)
  })
})

describe('isLifecycleExecutionEnabled', () => {
  it.each([
    { multitenant: true, lifecycle: false, backend: 's3', enabled: true },
    { multitenant: false, lifecycle: true, backend: 's3', enabled: true },
    { multitenant: false, lifecycle: false, backend: 's3', enabled: false },
    { multitenant: true, lifecycle: true, backend: 'file', enabled: true },
    { multitenant: true, lifecycle: false, backend: 'file', enabled: true },
    { multitenant: false, lifecycle: true, backend: 'file', enabled: true },
    { multitenant: false, lifecycle: false, backend: 'file', enabled: false },
  ] as const)('enabled=$enabled with multitenant=$multitenant lifecycle=$lifecycle backend=$backend', ({
    multitenant,
    lifecycle,
    backend,
    enabled,
  }) => {
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      isMultitenant: multitenant,
      storageLifecycleEnabled: lifecycle,
      storageBackendType: backend,
    })

    expect(isLifecycleExecutionEnabled()).toBe(enabled)
  })
})

it.each([true, false])('requires the lifecycle schema (installed=%s)', async (installed) => {
  const database = { hasMigration: vi.fn().mockResolvedValue(installed) }
  const result = assertLifecycleSchemaReady(database, 'avatars')
  if (installed) await expect(result).resolves.toBeUndefined()
  else await expect(result).rejects.toMatchObject({ code: 'FeatureNotEnabled' })
  expect(database.hasMigration).toHaveBeenCalledWith('bucket-lifecycle-configuration')
})
