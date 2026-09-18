import { StorageBackendError } from '@internal/errors'
import { vi } from 'vitest'

const { createStorage, loggerError, loggerInfo, loggerWarn, logEvent, S3Backend } = vi.hoisted(
  () => ({
    createStorage: vi.fn(),
    loggerError: vi.fn(),
    loggerInfo: vi.fn(),
    loggerWarn: vi.fn(),
    logEvent: vi.fn(),
    S3Backend: class {},
  })
)

vi.mock('../../../config', () => ({
  getConfig: () => ({ storageS3Bucket: 'test-storage' }),
}))

vi.mock('../base-event', () => ({
  BaseEvent: class {
    static createStorage = createStorage
  },
}))

vi.mock('@internal/monitoring', () => ({
  logger: { error: loggerError, info: loggerInfo, warn: loggerWarn },
  logSchema: { event: logEvent },
}))

vi.mock('@storage/backend', async () => {
  const { isMissingBackendObject, withOptionalVersion } = await import('../../backend/adapter')
  return { S3Backend, isMissingBackendObject, withOptionalVersion }
})

import { BackupObjectEvent } from './backup-object'
import { ObjectAdminDelete } from './object-admin-delete'

const job = {
  id: 'backup-object-job',
  data: {
    tenant: { ref: 'tenant-a', host: 'tenant-a.example.test' },
    bucketId: 'bucket-a',
    name: 'object-a',
    version: 'version-a',
    size: 1,
  },
} as Parameters<typeof BackupObjectEvent.handle>[0]

const deleteJob = { ...job, data: { ...job.data, deleteOriginal: true } }
const missingSource = Object.assign(new Error('source missing'), {
  name: 'NoSuchKey',
  $metadata: { httpStatusCode: 404 },
})

describe('BackupObjectEvent', () => {
  const backup = vi.fn()
  const deleteObject = vi.fn()
  const deleteObjects = vi.fn()
  const headObject = vi.fn()
  const destroyConnection = vi.fn()

  beforeEach(() => {
    vi.resetAllMocks()
    const backend = Object.assign(new S3Backend(), {
      backup,
      deleteObject,
      deleteObjects,
      headObject,
    })

    createStorage.mockResolvedValue({
      backend,
      db: { destroyConnection },
      location: {
        getKeyLocation: vi.fn().mockReturnValue('tenant-a/bucket-a/object-a'),
      },
    })
  })

  it('disposes the connection when skipping a non-S3 backend', async () => {
    createStorage.mockResolvedValue({
      backend: {},
      db: { destroyConnection },
      location: {
        getKeyLocation: vi.fn().mockReturnValue('tenant-a/bucket-a/object-a'),
      },
    })

    await expect(BackupObjectEvent.handle(job)).resolves.toBeUndefined()

    expect(destroyConnection).toHaveBeenCalledExactlyOnceWith()
    expect(logEvent).not.toHaveBeenCalled()
    expect(backup).not.toHaveBeenCalled()
    expect(headObject).not.toHaveBeenCalled()
  })

  it('rejects backup failures after logging them and disposing its database connection', async () => {
    const failure = new Error('backup failed')
    backup.mockRejectedValue(failure)

    await expect(BackupObjectEvent.handle(job)).rejects.toBe(failure)

    expect(loggerError).toHaveBeenCalledWith(
      expect.objectContaining({
        error: failure,
        jobId: 'backup-object-job',
        event: 'BackupObject',
      }),
      '[Admin]: BackupObjectEvent tenant-a/bucket-a/object-a - FAILED'
    )
    expect(destroyConnection).toHaveBeenCalledTimes(1)
  })

  it('accepts a retry after deletion succeeded but its response was lost', async () => {
    const failure = new Error('delete response timeout')
    let sourceExists = true
    let backupExists = false
    backup.mockImplementation(async () => {
      if (!sourceExists) throw missingSource
      backupExists = true
    })
    deleteObject.mockImplementation(async () => {
      sourceExists = false
      throw failure
    })
    headObject.mockImplementation(async () => {
      if (!backupExists) throw new Error('backup missing')
      return { size: job.data.size }
    })

    await expect(BackupObjectEvent.handle(deleteJob)).rejects.toBe(failure)
    expect(sourceExists).toBe(false)
    expect(backupExists).toBe(true)
    expect(headObject).not.toHaveBeenCalled()

    await expect(BackupObjectEvent.handle(deleteJob)).resolves.toBeUndefined()

    expect(headObject).toHaveBeenCalledExactlyOnceWith(
      'test-storage',
      '__internal/tenant-a/bucket-a/object-a/version-a',
      undefined,
      { confirmMissing: true }
    )
    expect(loggerInfo).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        jobId: job.id,
        objectVersion: job.data.version,
        backupKey: '__internal/tenant-a/bucket-a/object-a/version-a',
        outcome: 'already_backed_up',
      }),
      '[Admin]: BackupObjectEvent tenant-a/bucket-a/object-a - ALREADY BACKED UP'
    )
    expect(deleteObject).toHaveBeenCalledTimes(1)
    expect(loggerError).toHaveBeenCalledTimes(1)
    expect(destroyConnection).toHaveBeenCalledTimes(2)
  })

  it('warns and completes when an admin delete removes the source before backup runs', async () => {
    let sourceExists = true
    deleteObjects.mockImplementation(async () => {
      sourceExists = false
    })
    backup.mockImplementation(async () => {
      if (!sourceExists) throw missingSource
    })
    headObject.mockRejectedValue(StorageBackendError.fromError(missingSource))

    await ObjectAdminDelete.handle(deleteJob)
    expect(sourceExists).toBe(false)
    expect(deleteObjects).toHaveBeenCalledExactlyOnceWith('test-storage', [
      'tenant-a/bucket-a/object-a/version-a',
      'tenant-a/bucket-a/object-a/version-a.info',
    ])

    await expect(BackupObjectEvent.handle(deleteJob)).resolves.toBeUndefined()

    expect(loggerWarn).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        jobId: job.id,
        objectVersion: job.data.version,
        backupKey: '__internal/tenant-a/bucket-a/object-a/version-a',
        outcome: 'source_and_backup_missing',
      }),
      '[Admin]: BackupObjectEvent tenant-a/bucket-a/object-a - SKIPPED: source and backup are missing'
    )
    expect(loggerError).not.toHaveBeenCalled()
    expect(deleteObject).not.toHaveBeenCalled()
    expect(destroyConnection).toHaveBeenCalledTimes(2)
  })

  it('retries copying and deletion when deletion failed before taking effect', async () => {
    const failure = new Error('delete unavailable')
    backup.mockResolvedValue(undefined)
    deleteObject.mockRejectedValueOnce(failure).mockResolvedValueOnce(undefined)

    await expect(BackupObjectEvent.handle(deleteJob)).rejects.toBe(failure)
    await expect(BackupObjectEvent.handle(deleteJob)).resolves.toBeUndefined()

    expect(backup).toHaveBeenCalledTimes(2)
    expect(backup).toHaveBeenLastCalledWith({
      sourceBucket: 'test-storage',
      destinationBucket: 'test-storage',
      sourceKey: 'tenant-a/bucket-a/object-a/version-a',
      destinationKey: '__internal/tenant-a/bucket-a/object-a/version-a',
      size: job.data.size,
    })
    expect(deleteObject).toHaveBeenCalledTimes(2)
    expect(headObject).not.toHaveBeenCalled()
    expect(destroyConnection).toHaveBeenCalledTimes(2)
  })

  it.each([
    ['NoSuchBucket', 404],
    ['NotFound', 404],
    ['AccessDenied', 403],
    ['NoSuchKey', 503],
  ])('preserves %s/%s errors without accepting an existing backup', async (name, status) => {
    const failure = Object.assign(new Error(name), {
      name,
      $metadata: { httpStatusCode: status },
    })
    backup.mockRejectedValue(failure)
    headObject.mockResolvedValue({ size: job.data.size })

    await expect(BackupObjectEvent.handle(deleteJob)).rejects.toBe(failure)

    expect(headObject).not.toHaveBeenCalled()
    expect(deleteObject).not.toHaveBeenCalled()
    expect(destroyConnection).toHaveBeenCalledTimes(1)
  })

  it.each([
    ['NotFound', 404],
    ['NoSuchBucket', 404],
    ['NoSuchKey', 503],
    ['AccessDenied', 403],
    ['TimeoutError', undefined],
  ])('keeps the job failed when verification fails: %s', async (name, status) => {
    const failure = StorageBackendError.fromError(
      Object.assign(new Error(name), { name, $metadata: { httpStatusCode: status } })
    )
    backup.mockRejectedValue(missingSource)
    headObject.mockRejectedValue(failure)

    await expect(BackupObjectEvent.handle(deleteJob)).rejects.toBe(failure)

    expect(deleteObject).not.toHaveBeenCalled()
    expect(loggerError).toHaveBeenCalledWith(
      expect.objectContaining({ error: failure }),
      expect.any(String)
    )
    expect(destroyConnection).toHaveBeenCalledTimes(1)
  })

  it('rejects a backup whose size differs from the queued revision', async () => {
    backup.mockRejectedValue(missingSource)
    headObject.mockResolvedValue({ size: job.data.size + 1 })

    await expect(BackupObjectEvent.handle(deleteJob)).rejects.toThrow(
      'Backup size mismatch for __internal/tenant-a/bucket-a/object-a/version-a: expected 1 bytes, found 2'
    )

    expect(loggerError).toHaveBeenCalledWith(
      expect.objectContaining({
        backupKey: '__internal/tenant-a/bucket-a/object-a/version-a',
        error: expect.objectContaining({
          message: expect.stringContaining('expected 1 bytes, found 2'),
          cause: missingSource,
        }),
      }),
      expect.any(String)
    )
    expect(deleteObject).not.toHaveBeenCalled()
    expect(destroyConnection).toHaveBeenCalledTimes(1)
  })

  it('preserves missing-source failures for backup-only jobs', async () => {
    backup.mockRejectedValue(missingSource)
    headObject.mockResolvedValue({ size: job.data.size })

    await expect(BackupObjectEvent.handle(job)).rejects.toBe(missingSource)

    expect(headObject).not.toHaveBeenCalled()
    expect(deleteObject).not.toHaveBeenCalled()
  })
})
