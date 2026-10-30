import * as tenants from '@internal/database'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as config from '../../config'
import {
  assertLifecycleSchemaReady,
  assertLifecycleWriteReady,
  isLifecycleEnabledForTenant,
} from './guards'

afterEach(() => {
  vi.restoreAllMocks()
  vi.unstubAllEnvs()
  config.getConfig({ reload: true })
})

describe('lifecycle versioning gate', () => {
  it.each([
    { multitenant: false, versioning: true, feature: false, allowed: true },
    { multitenant: false, versioning: false, feature: true, allowed: false },
    { multitenant: true, versioning: false, feature: true, allowed: true },
    { multitenant: true, versioning: true, feature: false, allowed: true },
    { multitenant: true, versioning: false, feature: false, allowed: false },
  ])(
    'allows=$allowed for multitenant=$multitenant versioning=$versioning feature=$feature',
    async ({ multitenant, versioning, feature, allowed }) => {
      vi.spyOn(config, 'getConfig').mockReturnValue({
        ...config.getConfig(),
        isMultitenant: multitenant,
        storageVersioningEnabled: versioning,
      })
      const hasFeature = vi.spyOn(tenants, 'tenantHasFeature').mockResolvedValue(feature)
      expect(await isLifecycleEnabledForTenant('tenant-a')).toBe(allowed)
      if (versioning || !multitenant) expect(hasFeature).not.toHaveBeenCalled()
      else expect(hasFeature).toHaveBeenCalledWith('tenant-a', 'objectVersioning')
    }
  )

  it('rechecks the versioning flag after configuration reload', async () => {
    vi.stubEnv('MULTI_TENANT', 'false')
    vi.stubEnv('STORAGE_VERSIONING_ENABLED', 'false')
    config.getConfig({ reload: true })
    expect(await isLifecycleEnabledForTenant('tenant-a')).toBe(false)
    vi.stubEnv('STORAGE_VERSIONING_ENABLED', 'true')
    config.getConfig({ reload: true })
    expect(await isLifecycleEnabledForTenant('tenant-a')).toBe(true)
  })

  it('rejects writes before checking migrations when the feature is disabled', async () => {
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      isMultitenant: false,
      storageVersioningEnabled: false,
    })
    const database = { tenantId: 'tenant-a', hasMigration: vi.fn() }
    await expect(assertLifecycleWriteReady(database, 'avatars')).rejects.toMatchObject({
      code: 'FeatureNotEnabled',
    })
    expect(database.hasMigration).not.toHaveBeenCalled()
  })

  it.each([true, false])(
    'requires lifecycle writer coordination (installed=%s)',
    async (installed) => {
      const database = { hasMigration: vi.fn().mockResolvedValue(installed) }
      const result = assertLifecycleSchemaReady(database, 'avatars')
      if (installed) await expect(result).resolves.toBeUndefined()
      else await expect(result).rejects.toMatchObject({ code: 'FeatureNotEnabled' })
      expect(database.hasMigration).toHaveBeenCalledWith('lifecycle-writer-coordination')
    }
  )
})
