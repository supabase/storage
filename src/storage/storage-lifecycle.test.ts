import * as tenants from '@internal/database'
import { logSchema } from '@internal/monitoring'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import * as config from '../config'
import type { Database } from './database'
import { LIFECYCLE_PRE_COMMIT_WAKE_DELAY_MS } from './lifecycle/control-plane'
import type { BucketLifecycleConfiguration } from './schemas'
import { Storage } from './storage'

const policy: BucketLifecycleConfiguration = {
  rules: [{ status: 'Enabled', filter: {}, noncurrentVersionExpiration: { noncurrentDays: 1 } }],
}

function fixture() {
  const database = {
    tenantId: 'tenant-a',
    reqId: 'request-a',
    sbReqId: 'sb-request-a',
    putLifecycleConfiguration: vi.fn().mockResolvedValue({ lifecycle_configuration: policy }),
    deleteLifecycleConfiguration: vi.fn().mockResolvedValue(undefined),
    updateBucket: vi.fn().mockResolvedValue(undefined),
    findBucketById: vi.fn().mockResolvedValue({ type: 'STANDARD' }),
    hasMigration: vi.fn().mockResolvedValue(true),
  }
  return {
    database,
    storage: new Storage({} as never, database as unknown as Database, {} as never),
  }
}

describe('Storage lifecycle coordination', () => {
  beforeEach(() => {
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      storageBackendType: 's3',
      storageVersioningEnabled: false,
      isMultitenant: true,
    })
    vi.spyOn(tenants, 'tenantHasFeature').mockResolvedValue(true)
    vi.spyOn(tenants.LifecycleTenantStorePg.prototype, 'wakeTenant').mockResolvedValue()
  })

  it.each(['put', 'delete', 'enable versioning'] as const)(
    'coordinates central wake around REST %s with its own deadline',
    async (method) => {
      const timeout = vi.spyOn(AbortSignal, 'timeout')
      const { storage, database } = fixture()
      const events: string[] = []
      const wake = vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)
      wake.mockImplementation(async (_tenantId, _signal, delayMs) => {
        events.push(delayMs ? 'pre-wake' : 'wake')
      })
      if (method === 'put') {
        database.putLifecycleConfiguration.mockImplementation(async () => {
          events.push('mutation')
          return { lifecycle_configuration: policy }
        })
        await expect(storage.putBucketLifecycle('bucket', policy)).resolves.toEqual(policy)
        expect(database.putLifecycleConfiguration).toHaveBeenCalledWith('bucket', policy)
      } else if (method === 'delete') {
        database.deleteLifecycleConfiguration.mockImplementation(async () => {
          events.push('mutation')
        })
        await storage.deleteBucketLifecycle('bucket')
        expect(database.deleteLifecycleConfiguration).toHaveBeenCalledWith('bucket')
      } else {
        database.updateBucket.mockImplementation(async () => {
          events.push('mutation')
        })
        await storage.updateBucket('bucket', { versioning_status: 'ENABLED' })
        expect(database.updateBucket).toHaveBeenCalledWith(
          'bucket',
          expect.objectContaining({ versioning_status: 'ENABLED' })
        )
      }

      if (method === 'delete') {
        expect(events).toEqual(['mutation', 'wake'])
        expect(wake).toHaveBeenCalledExactlyOnceWith('tenant-a', expect.any(AbortSignal))
      } else {
        expect(events).toEqual(['pre-wake', 'mutation', 'wake'])
        expect(wake).toHaveBeenNthCalledWith(
          1,
          'tenant-a',
          expect.any(AbortSignal),
          LIFECYCLE_PRE_COMMIT_WAKE_DELAY_MS
        )
        expect(wake).toHaveBeenNthCalledWith(2, 'tenant-a', expect.any(AbortSignal))
      }
      expect(timeout).toHaveBeenCalledWith(5000)
    }
  )

  it('does not touch the central registry outside multitenant mode', async () => {
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      storageVersioningEnabled: true,
      isMultitenant: false,
    })
    const { storage, database } = fixture()

    await storage.putBucketLifecycle('bucket', policy)

    expect(database.putLifecycleConfiguration).toHaveBeenCalledOnce()
    expect(tenants.LifecycleTenantStorePg.prototype.wakeTenant).not.toHaveBeenCalled()
  })

  it('still wakes committed work after request cancellation', async () => {
    const { storage, database } = fixture()
    const requestController = new AbortController()
    database.putLifecycleConfiguration.mockImplementation(async () => {
      requestController.abort()
      return { lifecycle_configuration: policy }
    })
    const wake = vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)

    await expect(storage.putBucketLifecycle('bucket', policy)).resolves.toEqual(policy)
    expect(wake).toHaveBeenCalledTimes(2)
    const signal = wake.mock.calls[1][1]
    expect(signal).toBeInstanceOf(AbortSignal)
    expect(signal).not.toBe(requestController.signal)
    expect(signal?.aborted).toBe(false)
  })

  it.each(['unavailable registry', 'wake deadline'])(
    'preserves the committed mutation after a post-commit %s failure',
    async (cause) => {
      const wakeDeadline = new AbortController()
      vi.spyOn(AbortSignal, 'timeout')
        .mockReturnValueOnce(new AbortController().signal)
        .mockReturnValueOnce(wakeDeadline.signal)
      const { storage } = fixture()
      const failure = new Error(cause)
      const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
      const wake = vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)
      wake.mockResolvedValueOnce().mockImplementationOnce((_tenantId, signal) => {
        if (cause === 'unavailable registry') return Promise.reject(failure)
        const pending = new Promise<void>((_resolve, reject) => {
          signal!.addEventListener('abort', () => reject(signal!.reason), { once: true })
        })
        wakeDeadline.abort(failure)
        return pending
      })

      await expect(storage.putBucketLifecycle('bucket', policy)).resolves.toEqual(policy)
      expect(wake).toHaveBeenCalledTimes(2)
      expect(warning).toHaveBeenCalledWith(
        expect.anything(),
        expect.stringContaining('wake lifecycle tenant after commit'),
        expect.objectContaining({
          error: failure,
          tenantId: 'tenant-a',
          reqId: 'request-a',
          sbReqId: 'sb-request-a',
        })
      )
    }
  )

  it.each(['put', 'enable versioning'] as const)(
    'aborts %s without saving when the pre-commit wake fails',
    async (method) => {
      const { storage, database } = fixture()
      const failure = new Error('central database unavailable')
      const wake = vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)
      wake.mockRejectedValueOnce(failure)

      if (method === 'put') {
        await expect(storage.putBucketLifecycle('bucket', policy)).rejects.toBe(failure)
        expect(database.putLifecycleConfiguration).not.toHaveBeenCalled()
      } else {
        await expect(storage.updateBucket('bucket', { versioning_status: 'ENABLED' })).rejects.toBe(
          failure
        )
        expect(database.updateBucket).not.toHaveBeenCalled()
      }
      expect(wake).toHaveBeenCalledOnce()
    }
  )

  it.each(['put', 'delete', 'enable versioning'] as const)(
    'does not send a post-commit wake after a failed %s mutation',
    async (method) => {
      const { storage, database } = fixture()
      const failure = new Error('transaction rolled back')
      const wake = vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)
      if (method === 'put') {
        database.putLifecycleConfiguration.mockRejectedValueOnce(failure)
        await expect(storage.putBucketLifecycle('bucket', policy)).rejects.toBe(failure)
      } else if (method === 'delete') {
        database.deleteLifecycleConfiguration.mockRejectedValueOnce(failure)
        await expect(storage.deleteBucketLifecycle('bucket')).rejects.toBe(failure)
      } else {
        database.updateBucket.mockRejectedValueOnce(failure)
        await expect(storage.updateBucket('bucket', { versioning_status: 'ENABLED' })).rejects.toBe(
          failure
        )
      }
      if (method === 'delete') {
        expect(wake).not.toHaveBeenCalled()
      } else {
        expect(wake).toHaveBeenCalledExactlyOnceWith(
          'tenant-a',
          expect.any(AbortSignal),
          LIFECYCLE_PRE_COMMIT_WAKE_DELAY_MS
        )
      }
    }
  )

  it('does not wake for non-versioning bucket updates', async () => {
    const { storage, database } = fixture()

    await storage.updateBucket('bucket', { public: true })

    expect(database.updateBucket).toHaveBeenCalledOnce()
    expect(tenants.LifecycleTenantStorePg.prototype.wakeTenant).not.toHaveBeenCalled()
  })

  it('does not wake when enabling versioning before the writer-coordination migration', async () => {
    const { storage, database } = fixture()
    database.hasMigration.mockImplementation(
      async (name: string) => name !== 'lifecycle-writer-coordination'
    )

    await storage.updateBucket('bucket', { versioning_status: 'ENABLED' })

    expect(database.hasMigration).toHaveBeenCalledWith('lifecycle-writer-coordination')
    expect(database.updateBucket).toHaveBeenCalledOnce()
    expect(tenants.LifecycleTenantStorePg.prototype.wakeTenant).not.toHaveBeenCalled()
  })
})
