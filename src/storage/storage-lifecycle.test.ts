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
    connection: {
      getAbortSignal: vi.fn<() => AbortSignal | undefined>().mockReturnValue(undefined),
    },
    putLifecycleConfiguration: vi.fn().mockResolvedValue({ lifecycle_configuration: policy }),
    deleteLifecycleConfiguration: vi.fn().mockResolvedValue(undefined),
    updateBucket: vi.fn().mockResolvedValue(undefined),
    findBucketById: vi.fn().mockResolvedValue({ type: 'STANDARD' }),
    hasMigration: vi.fn().mockResolvedValue(true),
    withTransaction: vi.fn(async (fn: (db: unknown) => Promise<unknown>) => fn(database)),
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
      database.withTransaction.mockImplementation(async (fn) => {
        const result = await fn(database)
        events.push('commit')
        return result
      })
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
        expect(events).toEqual(['mutation', 'pre-wake', 'commit', 'wake'])
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
    database.connection.getAbortSignal.mockReturnValue(requestController.signal)
    database.withTransaction.mockImplementation(async (fn) => {
      const result = await fn(database)
      requestController.abort()
      return result
    })
    const wake = vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)

    await expect(storage.putBucketLifecycle('bucket', policy)).resolves.toEqual(policy)
    expect(wake).toHaveBeenCalledTimes(2)
    const signal = wake.mock.calls[1][1]
    expect(signal).toBeInstanceOf(AbortSignal)
    expect(signal).not.toBe(requestController.signal)
    expect(signal?.aborted).toBe(false)
  })

  it('rolls back instead of committing when the request aborts during pre-commit wake', async () => {
    const { storage, database } = fixture()
    const requestController = new AbortController()
    database.connection.getAbortSignal.mockReturnValue(requestController.signal)
    const events: string[] = []
    database.withTransaction.mockImplementation(async (fn) => {
      try {
        const result = await fn(database)
        events.push('commit')
        return result
      } catch (error) {
        events.push('rollback')
        throw error
      }
    })
    const abortReason = new Error('client disconnected')
    const wake = vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)
    wake.mockImplementationOnce(async (_tenantId, signal) => {
      expect(signal?.aborted).toBe(false)
      requestController.abort(abortReason)
      expect(signal?.aborted).toBe(true)
    })

    await expect(storage.putBucketLifecycle('bucket', policy)).rejects.toBe(abortReason)
    expect(events).toEqual(['rollback'])
    expect(wake).toHaveBeenCalledOnce()
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
    'rolls back %s without committing when the pre-commit wake fails',
    async (method) => {
      const { storage, database } = fixture()
      const events: string[] = []
      database.withTransaction.mockImplementation(async (fn) => {
        try {
          const result = await fn(database)
          events.push('commit')
          return result
        } catch (error) {
          events.push('rollback')
          throw error
        }
      })
      const failure = new Error('central database unavailable')
      const wake = vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)
      wake.mockRejectedValueOnce(failure)

      if (method === 'put') {
        database.putLifecycleConfiguration.mockImplementationOnce(async () => {
          events.push('mutation')
          return { lifecycle_configuration: policy }
        })
        await expect(storage.putBucketLifecycle('bucket', policy)).rejects.toMatchObject({
          code: 'InternalError',
          httpStatusCode: 500,
          originalError: failure,
        })
      } else {
        database.updateBucket.mockImplementationOnce(async () => {
          events.push('mutation')
        })
        await expect(
          storage.updateBucket('bucket', { versioning_status: 'ENABLED' })
        ).rejects.toMatchObject({
          code: 'InternalError',
          httpStatusCode: 500,
          originalError: failure,
        })
      }
      expect(events).toEqual(['mutation', 'rollback'])
      expect(wake).toHaveBeenCalledOnce()
    }
  )

  it.each(['put', 'delete', 'enable versioning'] as const)(
    'does not touch the central registry after a failed or unauthorized %s mutation',
    async (method) => {
      const { storage, database } = fixture()
      const failure = new Error('permission denied')
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
      expect(wake).not.toHaveBeenCalled()
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
