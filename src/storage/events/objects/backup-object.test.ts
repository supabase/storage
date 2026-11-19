import { StorageBackendError } from '@internal/errors'
import type { QueuedPayload, StorageJob } from '@internal/queue'
import { vi } from 'vitest'
import type { BackupObjectEventPayload } from './backup-object'

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

// The handler reads the real `isSyncDispatch` from `@internal/queue`, whose import graph needs
// a complete config — so only the bucket is overridden.
vi.mock('../../../config', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../config')>()
  return {
    ...actual,
    getConfig: () => ({ ...actual.getConfig(), storageS3Bucket: 'test-storage' }),
  }
})

// Minimal stand-in for base.ts: `createStorage` is the seam under test; `storageEvent` only
// needs enough class surface for QueueHandler.
vi.mock('../base', () => ({
  createStorage,
  storageEvent: (opts: { type: string }) =>
    class {
      static readonly eventType = opts.type
      constructor(readonly data: unknown) {}
    },
}))

vi.mock('../registry', () => ({
  QUEUES: { backupObject: 'backup-object' },
  backupRetry: (name: string) => ({
    maxAttempts: 6,
    backoffMs: 5_000,
    deadLetter: `${name}-dead-letter`,
  }),
}))

vi.mock('@internal/monitoring', () => ({
  logger: { error: loggerError, info: loggerInfo, warn: loggerWarn },
  logSchema: { event: logEvent },
}))

vi.mock('@storage/backend', async () => {
  const { isMissingBackendObject } = await import('../../backend/adapter')
  return { S3Backend, isMissingBackendObject }
})

import { BackupObjectHandler } from './backup-object'

type BackupCtx = StorageJob<BackupObjectEventPayload>

function makeCtx(
  data: Partial<QueuedPayload<BackupObjectEventPayload>> = {},
  headers: Record<string, string> = {}
): BackupCtx {
  return {
    queue: 'backup-object',
    message: {
      id: 'backup-object-job',
      data: {
        tenant: { ref: 'tenant-a', host: 'tenant-a.example.test' },
        bucketId: 'bucket-a',
        name: 'object-a',
        version: 'version-a',
        size: 1,
        region: 'local',
        ...data,
      },
      headers,
      timestamp: 0,
      attempt: 1,
    },
    signal: new AbortController().signal,
    heartbeat: async () => {},
  }
}

const job = makeCtx()
const deleteJob = makeCtx({ deleteOriginal: true })
// v1 marked an inline run with the `__sync` job id; v2 stamps this header instead.
const syncDeleteJob = makeCtx({ deleteOriginal: true }, { 'x-sync-dispatch': '1' })
const backupKey = '__internal/tenant-a/bucket-a/object-a/version-a'
const missingSource = Object.assign(new Error('source missing'), {
  name: 'NoSuchKey',
  $metadata: { httpStatusCode: 404 },
})

describe('BackupObjectHandler', () => {
  const handler = new BackupObjectHandler()
  const backup = vi.fn()
  const deleteObject = vi.fn()
  const headObject = vi.fn()
  const destroyConnection = vi.fn()

  beforeEach(() => {
    vi.resetAllMocks()
    const backend = Object.assign(new S3Backend(), {
      backup,
      deleteObject,
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

    await expect(handler.handle(job)).resolves.toBeUndefined()

    expect(destroyConnection).toHaveBeenCalledExactlyOnceWith()
    expect(logEvent).not.toHaveBeenCalled()
    expect(backup).not.toHaveBeenCalled()
    expect(headObject).not.toHaveBeenCalled()
  })

  it('rejects backup failures after logging them and disposing its database connection', async () => {
    const failure = new Error('backup failed')
    backup.mockRejectedValue(failure)

    await expect(handler.handle(job)).rejects.toBe(failure)

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

  it('logs synchronous backup failures and completes without deleting the source', async () => {
    const failure = Object.assign(new Error('Access denied'), {
      name: 'AccessDenied',
      $metadata: { httpStatusCode: 403 },
    })
    backup.mockRejectedValue(failure)

    await expect(handler.handle(syncDeleteJob)).resolves.toBeUndefined()

    expect(loggerError).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        error: failure,
        jobId: 'backup-object-job',
        event: 'BackupObject',
      }),
      '[Admin]: BackupObjectEvent tenant-a/bucket-a/object-a - FAILED'
    )
    expect(deleteObject).not.toHaveBeenCalled()
    expect(destroyConnection).toHaveBeenCalledTimes(1)
  })

  it('backs up and then deletes the source', async () => {
    await expect(handler.handle(deleteJob)).resolves.toBeUndefined()

    expect(backup).toHaveBeenCalledExactlyOnceWith({
      sourceBucket: 'test-storage',
      destinationBucket: 'test-storage',
      sourceKey: 'tenant-a/bucket-a/object-a/version-a',
      destinationKey: backupKey,
      size: job.message.data.size,
    })
    expect(deleteObject).toHaveBeenCalledExactlyOnceWith(
      'test-storage',
      'tenant-a/bucket-a/object-a',
      'version-a'
    )
    expect(headObject).not.toHaveBeenCalled()
  })

  it('accepts an existing backup when a retry finds the source already deleted', async () => {
    backup.mockRejectedValue(missingSource)
    headObject.mockResolvedValue({ size: job.message.data.size })

    await expect(handler.handle(deleteJob)).resolves.toBeUndefined()

    expect(headObject).toHaveBeenCalledExactlyOnceWith('test-storage', backupKey, undefined, {
      confirmMissing: true,
    })
    expect(loggerInfo).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ backupKey, outcome: 'already_backed_up' }),
      expect.any(String)
    )
    expect(deleteObject).not.toHaveBeenCalled()
  })

  it('warns and completes when both the source and backup are missing', async () => {
    backup.mockRejectedValue(missingSource)
    headObject.mockRejectedValue(StorageBackendError.fromError(missingSource))

    await expect(handler.handle(deleteJob)).resolves.toBeUndefined()

    expect(loggerWarn).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ backupKey, outcome: 'source_and_backup_missing' }),
      expect.any(String)
    )
    expect(loggerError).not.toHaveBeenCalled()
    expect(deleteObject).not.toHaveBeenCalled()
  })

  it.each([
    ['NoSuchBucket', 404],
    ['NoSuchKey', 503],
  ])('rejects %s/%s from the copy without checking for a backup', async (name, status) => {
    const failure = Object.assign(new Error(name), { name, $metadata: { httpStatusCode: status } })
    backup.mockRejectedValue(failure)

    await expect(handler.handle(deleteJob)).rejects.toBe(failure)

    expect(headObject).not.toHaveBeenCalled()
  })

  it('rejects when the backup cannot be confirmed missing', async () => {
    const failure = StorageBackendError.fromError(
      Object.assign(new Error('NotFound'), { name: 'NotFound', $metadata: { httpStatusCode: 404 } })
    )
    backup.mockRejectedValue(missingSource)
    headObject.mockRejectedValue(failure)

    await expect(handler.handle(deleteJob)).rejects.toBe(failure)

    expect(loggerWarn).not.toHaveBeenCalled()
  })

  it('rejects a missing source for backup-only jobs', async () => {
    backup.mockRejectedValue(missingSource)

    await expect(handler.handle(job)).rejects.toBe(missingSource)

    expect(headObject).not.toHaveBeenCalled()
  })
})
