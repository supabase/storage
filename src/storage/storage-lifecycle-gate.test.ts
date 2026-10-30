import * as tenants from '@internal/database'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as config from '../config'
import { Storage } from './storage'

const policy = {
  rules: [
    { status: 'Enabled' as const, filter: {}, noncurrentVersionExpiration: { noncurrentDays: 1 } },
  ],
}

afterEach(() => vi.restoreAllMocks())

describe('lifecycle configuration access', () => {
  it.each(['get', 'put', 'delete'] as const)(
    'blocks %s before database access for a disabled tenant',
    async (method) => {
      vi.spyOn(config, 'getConfig').mockReturnValue({
        ...config.getConfig(),
        isMultitenant: true,
        storageVersioningEnabled: false,
      })
      const hasFeature = vi.spyOn(tenants, 'tenantHasFeature').mockResolvedValue(false)
      const db = {
        tenantId: 'tenant-a',
        findLifecycleBucket: vi.fn(),
        putLifecycleConfiguration: vi.fn(),
        deleteLifecycleConfiguration: vi.fn(),
      }
      const storage = new Storage({} as never, db as never, {} as never)
      const operation =
        method === 'get'
          ? storage.getBucketLifecycle('bucket')
          : method === 'put'
            ? storage.putBucketLifecycle('bucket', policy)
            : storage.deleteBucketLifecycle('bucket')

      await expect(operation).rejects.toMatchObject({ code: 'FeatureNotEnabled' })
      expect(hasFeature).toHaveBeenCalledWith('tenant-a', 'objectVersioning')
      expect(db.findLifecycleBucket).not.toHaveBeenCalled()
      expect(db.putLifecycleConfiguration).not.toHaveBeenCalled()
      expect(db.deleteLifecycleConfiguration).not.toHaveBeenCalled()
    }
  )
})
