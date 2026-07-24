import { vi } from 'vitest'

const { createStorage, loggerError, logEvent, S3Backend } = vi.hoisted(() => ({
  createStorage: vi.fn(),
  loggerError: vi.fn(),
  logEvent: vi.fn(),
  S3Backend: class {},
}))

vi.mock('../../../config', () => ({
  getConfig: () => ({ storageS3Bucket: 'test-storage' }),
}))

vi.mock('../base-event', () => ({
  BaseEvent: class {
    static createStorage = createStorage
  },
}))

vi.mock('@internal/monitoring', () => ({
  logger: { error: loggerError },
  logSchema: { event: logEvent },
}))

vi.mock('@storage/backend', () => ({
  S3Backend,
}))

import { BackupObjectEvent } from './backup-object'

const job = {
  id: 'backup-object-job',
  data: {
    tenant: { ref: 'tenant-a', host: 'tenant-a.example.test' },
    bucketId: 'bucket-a',
    name: 'object-a',
    version: 'version-a',
    size: 1,
  },
} as never

describe('BackupObjectEvent', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('rejects backup failures after logging them and disposing its database connection', async () => {
    const failure = new Error('backup failed')
    const destroyConnection = vi.fn().mockResolvedValue(undefined)
    const backend = Object.assign(new S3Backend(), {
      backup: vi.fn().mockRejectedValue(failure),
    })

    createStorage.mockResolvedValue({
      backend,
      db: { destroyConnection },
      location: {
        getKeyLocation: vi.fn().mockReturnValue('tenant-a/bucket-a/object-a'),
      },
    })

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
})
