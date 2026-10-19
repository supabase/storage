import { describe, expect, it, vi } from 'vitest'
import { ObjectRemoved } from '../lifecycle/object-removed'
import { ObjectAdminDeleteAllBefore } from './object-admin-delete-all-before'

vi.mock('../../database', () => ({
  StoragePgDB: class {},
}))
vi.mock('../../storage', () => ({
  Storage: class {},
}))

function createJob(overrides: Partial<{ before: string; bucketId: string }> = {}) {
  return {
    id: 'job-1',
    data: {
      before: overrides.before ?? new Date().toISOString(),
      bucketId: overrides.bucketId ?? 'bucket',
      tenant: { ref: 'tenant-id', host: 'localhost' },
      reqId: 'req-id',
      sbReqId: 'sb-req-id',
    },
  }
}

describe('ObjectAdminDeleteAllBefore.handle', () => {
  it('does not touch backend content or send a webhook when the metadata delete fails to commit', async () => {
    const backendDeleteObjectsDetailed = vi.fn()
    const db = {
      listObjects: vi.fn().mockResolvedValue([{ id: 'id-1', name: 'a.txt' }]),
      deleteObjects: vi.fn().mockRejectedValue(new Error('commit failed')),
      destroyConnection: vi.fn(),
    }
    const storage = {
      db,
      backend: { deleteObjectsDetailed: backendDeleteObjectsDetailed },
      location: {
        getRootLocation: vi.fn(() => 'root-bucket'),
        getKeyLocation: vi.fn(() => 'tenant-id/bucket/a.txt'),
      },
    }
    vi.spyOn(
      ObjectAdminDeleteAllBefore as unknown as { createStorage: () => Promise<unknown> },
      'createStorage'
    ).mockResolvedValue(storage)
    const sendWebhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)

    await expect(ObjectAdminDeleteAllBefore.handle(createJob() as never)).rejects.toThrow(
      'commit failed'
    )

    // The metadata delete never committed, so backend cleanup must never run
    // and no webhook may fire for content that was never actually removed.
    expect(backendDeleteObjectsDetailed).not.toHaveBeenCalled()
    expect(sendWebhook).not.toHaveBeenCalled()
    expect(db.destroyConnection).toHaveBeenCalled()
  })
})
