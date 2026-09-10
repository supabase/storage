import * as tenants from '@internal/database'
import { logSchema } from '@internal/monitoring'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
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
    updateBucket: vi.fn().mockResolvedValue({ previous: { public: false } }),
  }
  const database = {
    tenantId: 'tenant-a',
    tenant: () => ({ ref: 'tenant-a', host: '' }),
    findBucketById: vi.fn().mockResolvedValue({ id: 'bucket', type: 'STANDARD' }),
    hasMigration: vi.fn().mockResolvedValue(true),
    updateBucket: vi.fn().mockResolvedValue({ previous: { public: false } }),
    connection: {
      getAbortSignal: () => requestSignal,
      setAbortSignal: vi.fn(),
    },
    findLifecycleBucket: vi.fn().mockResolvedValue({ lifecycle_configuration: policy }),
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
      versioningEnabled: true,
      isMultitenant: true,
    })
    vi.spyOn(tenants, 'tenantHasFeature').mockResolvedValue(true)
    vi.spyOn(tenants.LifecycleTenantStorePg.prototype, 'wakeTenant').mockResolvedValue()
  })

  afterEach(() => vi.restoreAllMocks())

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
  ] as const)('wakes before and after REST %s commits, with separate deadlines', async (method) => {
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
    } else {
      await storage.deleteBucketLifecycle('bucket')
      expect(transaction.deleteLifecycleConfiguration).toHaveBeenCalledWith('bucket')
    }

    const operationSignal = database.connection.setAbortSignal.mock.calls[0][0]
    expect(events).toEqual(['wake', 'commit', 'wake'])
    expect(wake).toHaveBeenCalledTimes(2)
    expect(wake).toHaveBeenNthCalledWith(1, 'tenant-a', operationSignal)
    const postCommitSignal = wake.mock.calls[1][1]
    expect(postCommitSignal).toBeInstanceOf(AbortSignal)
    expect(postCommitSignal).not.toBe(operationSignal)
    expect(postCommitSignal).not.toBe(requestSignal)
    expect(operationSignal).toBeInstanceOf(AbortSignal)
    expect(operationSignal).not.toBe(requestSignal)
    expect(database.withTransaction).toHaveBeenCalledWith(expect.any(Function), {
      deadlineSignal: operationSignal,
    })
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
    vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)
      .mockResolvedValueOnce()
      .mockImplementationOnce(async (_tenantId, signal) => {
        signal!.throwIfAborted()
        committedWake = true
      })

    await expect(storage.putBucketLifecycle('bucket', policy)).resolves.toEqual(policy)
    expect(committedWake).toBe(true)
    expect(database.connection.setAbortSignal).toHaveBeenLastCalledWith(requestSignal)
  })

  it('bounds the independent wake without failing the committed mutation', async () => {
    const transactionDeadline = new AbortController()
    const wakeDeadline = new AbortController()
    const timeout = vi
      .spyOn(AbortSignal, 'timeout')
      .mockReturnValueOnce(transactionDeadline.signal)
      .mockReturnValueOnce(wakeDeadline.signal)
    const { storage, database, requestSignal } = fixture()
    const failure = new Error('post-commit wake deadline')
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)
      .mockResolvedValueOnce()
      .mockImplementationOnce((_tenantId, signal) => {
        const pending = new Promise<void>((_resolve, reject) => {
          signal!.addEventListener('abort', () => reject(signal!.reason), { once: true })
        })
        wakeDeadline.abort(failure)
        return pending
      })

    await expect(storage.putBucketLifecycle('bucket', policy)).resolves.toEqual(policy)
    expect(timeout).toHaveBeenLastCalledWith(5000)
    expect(warning).toHaveBeenCalledWith(
      expect.anything(),
      expect.stringContaining('wake lifecycle tenant after commit'),
      expect.objectContaining({ error: failure, tenantId: 'tenant-a' })
    )
    expect(database.connection.setAbortSignal).toHaveBeenLastCalledWith(requestSignal)
  })

  it('preserves the committed mutation when its second wake fails', async () => {
    const { storage, database, requestSignal } = fixture()
    const failure = new Error('central registry unavailable')
    const wake = vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant)
    wake.mockResolvedValueOnce().mockRejectedValueOnce(failure)
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})

    await expect(storage.putBucketLifecycle('bucket', policy)).resolves.toEqual(policy)
    expect(wake).toHaveBeenCalledTimes(2)
    expect(database.withTransaction).toHaveBeenCalledOnce()
    expect(database.connection.setAbortSignal).toHaveBeenLastCalledWith(requestSignal)
    expect(warning).toHaveBeenCalledWith(
      expect.anything(),
      expect.stringContaining('wake lifecycle tenant after commit'),
      expect.objectContaining({ error: failure, tenantId: 'tenant-a' })
    )
  })

  it('does not send the second wake after a failed transaction', async () => {
    const { storage, database, requestSignal } = fixture()
    const failure = new Error('transaction rolled back')
    database.withTransaction.mockRejectedValueOnce(failure)

    await expect(storage.putBucketLifecycle('bucket', policy)).rejects.toBe(failure)
    expect(tenants.LifecycleTenantStorePg.prototype.wakeTenant).toHaveBeenCalledOnce()
    expect(database.connection.setAbortSignal).toHaveBeenLastCalledWith(requestSignal)
  })

  it('does not enter the tenant transaction when the deadline expires during pre-wake', async () => {
    const controller = new AbortController()
    vi.spyOn(AbortSignal, 'timeout').mockReturnValue(controller.signal)
    const { storage, database, requestSignal } = fixture()
    vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant).mockImplementation(async () => {
      controller.abort(new Error('configuration deadline'))
    })

    await expect(storage.putBucketLifecycle('bucket', policy)).rejects.toThrow(
      'configuration deadline'
    )
    expect(database.withTransaction).not.toHaveBeenCalled()
    expect(database.connection.setAbortSignal).toHaveBeenLastCalledWith(requestSignal)
  })

  it('cancels a pending central wake when the configuration deadline expires', async () => {
    const controller = new AbortController()
    vi.spyOn(AbortSignal, 'timeout').mockReturnValue(controller.signal)
    const { storage, database, requestSignal } = fixture()
    vi.mocked(tenants.LifecycleTenantStorePg.prototype.wakeTenant).mockImplementation(
      async (_tenantId, signal) => {
        expect(signal).toBeInstanceOf(AbortSignal)
        const pending = new Promise<void>((_resolve, reject) => {
          signal!.addEventListener('abort', () => reject(signal!.reason), { once: true })
        })
        controller.abort(new Error('configuration deadline'))
        return pending
      }
    )

    await expect(storage.putBucketLifecycle('bucket', policy)).rejects.toThrow(
      'configuration deadline'
    )
    expect(database.withTransaction).not.toHaveBeenCalled()
    expect(database.connection.setAbortSignal).toHaveBeenLastCalledWith(requestSignal)
  })
})
