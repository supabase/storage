import * as tenants from '@internal/database'
import { logSchema } from '@internal/monitoring'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import * as config from '../config'
import type { Database } from './database'
import type { BucketLifecycleConfiguration } from './schemas'
import { Storage } from './storage'

const policy: BucketLifecycleConfiguration = {
  rules: [{ status: 'Enabled', filter: {}, noncurrentVersionExpiration: { noncurrentDays: 1 } }],
}

function fixture() {
  const requestController = new AbortController()
  const requestSignal = requestController.signal
  const transaction = {
    putLifecycleConfiguration: vi.fn().mockResolvedValue({ lifecycle_configuration: policy }),
    deleteLifecycleConfiguration: vi.fn(),
    updateBucket: vi.fn(),
  }
  const database = {
    tenantId: 'tenant-a',
    tenant: () => ({ ref: 'tenant-a', host: '' }),
    connection: {
      getAbortSignal: () => requestSignal,
      setAbortSignal: vi.fn(),
    },
    findLifecycleBucket: vi.fn().mockResolvedValue({ lifecycle_configuration: policy }),
    findBucketById: vi.fn().mockResolvedValue({ type: 'STANDARD' }),
    hasMigration: vi.fn().mockResolvedValue(true),
    updateBucket: vi.fn(),
    withTransaction: vi.fn((callback: (db: Database) => unknown) =>
      callback(transaction as unknown as Database)
    ),
  }
  return {
    database,
    transaction,
    requestController,
    requestSignal,
    storage: new Storage({} as never, database as unknown as Database, {} as never),
  }
}

describe('Storage lifecycle coordination', () => {
  beforeEach(() => {
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      storageBackendType: 's3',
      storageLifecycleEnabled: true,
      isMultitenant: true,
    })
    vi.spyOn(tenants, 'tenantHasFeature').mockResolvedValue(true)
    vi.spyOn(tenants.LifecycleTenantStorePg.prototype, 'wakeTenant').mockResolvedValue()
  })

  it.each([
    'get',
    'put',
    'delete',
  ] as const)('gates REST %s by the tenant feature', async (method) => {
    vi.mocked(tenants.tenantHasFeature).mockResolvedValue(false)
    const { storage, database } = fixture()
    const operation =
      method === 'get'
        ? storage.getBucketLifecycle('bucket')
        : method === 'put'
          ? storage.putBucketLifecycle('bucket', policy)
          : storage.deleteBucketLifecycle('bucket')

    await expect(operation).rejects.toMatchObject({ code: 'FeatureNotEnabled' })
    expect(tenants.tenantHasFeature).toHaveBeenCalledWith('tenant-a', 'objectVersioning')
    expect(database.findLifecycleBucket).not.toHaveBeenCalled()
    expect(database.withTransaction).not.toHaveBeenCalled()
    expect(tenants.LifecycleTenantStorePg.prototype.wakeTenant).not.toHaveBeenCalled()
  })

  it.each([
    'put',
    'delete',
    'enable versioning',
  ] as const)('wakes after REST %s commits with its own deadline', async (method) => {
    const timeout = vi.spyOn(AbortSignal, 'timeout')
    const { storage, database, transaction, requestSignal } = fixture()
    const events: string[] = []
    const wake = vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)
    wake.mockImplementation(async () => {
      events.push('wake')
    })
    database.withTransaction.mockImplementation(async (callback) => {
      const result = await callback(transaction as unknown as Database)
      events.push('commit')
      return result
    })

    if (method === 'put') {
      await expect(storage.putBucketLifecycle('bucket', policy)).resolves.toEqual(policy)
      expect(transaction.putLifecycleConfiguration).toHaveBeenCalledWith('bucket', policy)
    } else if (method === 'delete') {
      await storage.deleteBucketLifecycle('bucket')
      expect(transaction.deleteLifecycleConfiguration).toHaveBeenCalledWith('bucket')
    } else {
      await storage.updateBucket('bucket', { versioning_status: 'ENABLED' })
      expect(transaction.updateBucket).toHaveBeenCalledWith(
        'bucket',
        expect.objectContaining({
          versioning_status: 'ENABLED',
        })
      )
    }

    const operationSignal = database.connection.setAbortSignal.mock.calls[0][0]
    expect(timeout).toHaveBeenNthCalledWith(1, 30_000)
    expect(timeout).toHaveBeenNthCalledWith(2, 5000)
    expect(events).toEqual(['commit', 'wake'])
    expect(wake).toHaveBeenCalledOnce()
    const postCommitSignal = wake.mock.calls[0][1]
    expect(postCommitSignal).toBeInstanceOf(AbortSignal)
    expect(postCommitSignal).not.toBe(operationSignal)
    expect(postCommitSignal).not.toBe(requestSignal)
    expect(operationSignal).toBeInstanceOf(AbortSignal)
    expect(operationSignal).not.toBe(requestSignal)
    expect(database.withTransaction).toHaveBeenCalledWith(expect.any(Function))
    expect(database.connection.setAbortSignal).toHaveBeenLastCalledWith(requestSignal)
  })

  it.each([
    'request cancellation',
    'transaction deadline',
  ])('still wakes committed work after %s', async (cause) => {
    const transactionDeadline = new AbortController()
    vi.spyOn(AbortSignal, 'timeout').mockReturnValueOnce(transactionDeadline.signal)
    const { storage, database, transaction, requestController, requestSignal } = fixture()
    database.withTransaction.mockImplementation(async (callback) => {
      const result = await callback(transaction as unknown as Database)
      if (cause === 'request cancellation') requestController.abort()
      else transactionDeadline.abort()
      return result
    })
    let committedWake = false
    vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant).mockImplementationOnce(
      async (_tenantId, signal) => {
        signal!.throwIfAborted()
        committedWake = true
      }
    )

    await expect(storage.putBucketLifecycle('bucket', policy)).resolves.toEqual(policy)
    expect(committedWake).toBe(true)
    expect(database.connection.setAbortSignal).toHaveBeenLastCalledWith(requestSignal)
  })

  it.each([
    'unavailable registry',
    'wake deadline',
  ])('preserves the committed mutation after a post-commit %s failure', async (cause) => {
    const transactionDeadline = new AbortController()
    const wakeDeadline = new AbortController()
    const timeout = vi
      .spyOn(AbortSignal, 'timeout')
      .mockReturnValueOnce(transactionDeadline.signal)
      .mockReturnValueOnce(wakeDeadline.signal)
    const { storage, database, requestSignal } = fixture()
    const failure = new Error(cause)
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    const wake = vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)
    wake.mockImplementationOnce((_tenantId, signal) => {
      if (cause === 'unavailable registry') return Promise.reject(failure)
      const pending = new Promise<void>((_resolve, reject) => {
        signal!.addEventListener('abort', () => reject(signal!.reason), { once: true })
      })
      wakeDeadline.abort(failure)
      return pending
    })

    await expect(storage.putBucketLifecycle('bucket', policy)).resolves.toEqual(policy)
    expect(timeout).toHaveBeenLastCalledWith(5000)
    expect(wake).toHaveBeenCalledOnce()
    expect(database.withTransaction).toHaveBeenCalledOnce()
    expect(database.connection.setAbortSignal).toHaveBeenLastCalledWith(requestSignal)
    expect(warning).toHaveBeenCalledWith(
      expect.anything(),
      expect.stringContaining('wake lifecycle tenant after commit'),
      expect.objectContaining({ error: failure, tenantId: 'tenant-a' })
    )
  })

  it('supports lifecycle PUT, GET and DELETE on the file backend', async () => {
    vi.mocked(config.getConfig).mockReturnValue({
      ...config.getConfig(),
      storageBackendType: 'file',
    })
    const { storage, transaction } = fixture()

    await expect(storage.putBucketLifecycle('bucket', policy)).resolves.toEqual(policy)
    expect(transaction.putLifecycleConfiguration).toHaveBeenCalledWith('bucket', policy)
    expect(tenants.LifecycleTenantStorePg.prototype.wakeTenant).toHaveBeenCalledOnce()
    await expect(storage.getBucketLifecycle('bucket')).resolves.toEqual(policy)
    await storage.deleteBucketLifecycle('bucket')
    expect(transaction.deleteLifecycleConfiguration).toHaveBeenCalledWith('bucket')
  })

  it('does not wake after a failed transaction', async () => {
    const { storage, database, requestSignal } = fixture()
    const failure = new Error('transaction rolled back')
    database.withTransaction.mockRejectedValueOnce(failure)

    await expect(storage.putBucketLifecycle('bucket', policy)).rejects.toBe(failure)
    expect(tenants.LifecycleTenantStorePg.prototype.wakeTenant).not.toHaveBeenCalled()
    expect(database.connection.setAbortSignal).toHaveBeenLastCalledWith(requestSignal)
  })

  it.each([
    { mode: 'single-tenant', multitenant: false, flag: true, feature: false, allowed: true },
    { mode: 'single-tenant', multitenant: false, flag: false, feature: true, allowed: false },
    { mode: 'multitenant', multitenant: true, flag: false, feature: true, allowed: true },
    { mode: 'multitenant', multitenant: true, flag: true, feature: false, allowed: false },
    { mode: 'multitenant', multitenant: true, flag: false, feature: false, allowed: false },
  ])('$mode flag=$flag feature=$feature allowed=$allowed for lifecycle reads', async ({
    multitenant,
    flag,
    feature,
    allowed,
  }) => {
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      isMultitenant: multitenant,
      storageLifecycleEnabled: flag,
    })
    vi.mocked(tenants.tenantHasFeature).mockResolvedValue(feature)
    const { storage } = fixture()

    const result = storage.getBucketLifecycle('bucket')

    if (allowed) await expect(result).resolves.toEqual(policy)
    else await expect(result).rejects.toMatchObject({ code: 'FeatureNotEnabled' })
  })

  it('maps the transaction deadline to DatabaseTimeout but keeps request aborts raw', async () => {
    const { storage, database, transaction, requestController } = fixture()
    const deadline = new AbortController()
    vi.spyOn(AbortSignal, 'timeout').mockReturnValueOnce(deadline.signal)
    const aborted = new Error('Query was aborted')
    transaction.putLifecycleConfiguration.mockImplementation(async () => {
      deadline.abort(aborted)
      throw aborted
    })

    await expect(storage.putBucketLifecycle('bucket', policy)).rejects.toMatchObject({
      code: 'DatabaseTimeout',
      httpStatusCode: 544,
    })

    transaction.putLifecycleConfiguration.mockImplementation(async () => {
      requestController.abort()
      throw aborted
    })
    await expect(storage.putBucketLifecycle('bucket', policy)).rejects.toBe(aborted)
    expect(database.withTransaction).toHaveBeenCalledTimes(2)
  })

  it('does not coordinate non-versioning bucket updates', async () => {
    const { storage, database } = fixture()

    await storage.updateBucket('bucket', { public: true })

    expect(database.withTransaction).not.toHaveBeenCalled()
    expect(database.updateBucket).toHaveBeenCalledOnce()
    expect(tenants.LifecycleTenantStorePg.prototype.wakeTenant).not.toHaveBeenCalled()
  })

  it('updates plainly when enabling versioning before the writer-coordination migration', async () => {
    const { storage, database } = fixture()
    database.hasMigration.mockImplementation(
      async (name: string) => name !== 'lifecycle-writer-coordination'
    )

    await storage.updateBucket('bucket', { versioning_status: 'ENABLED' })

    expect(database.hasMigration).toHaveBeenCalledWith('lifecycle-writer-coordination')
    expect(database.withTransaction).not.toHaveBeenCalled()
    expect(database.updateBucket).toHaveBeenCalledOnce()
    expect(tenants.LifecycleTenantStorePg.prototype.wakeTenant).not.toHaveBeenCalled()
  })
})
