import { describe, expect, it, vi } from 'vitest'
import { MAX_OBJECTS_PER_DELETE_BATCH } from '../../limits'
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

  it('stamps applyTime before the backend cleanup resolves, not after', async () => {
    // A monotonic counter in place of the real clock to proves call order
    let now = 1_700_000_000_000
    const dateNowSpy = vi.spyOn(Date, 'now').mockImplementation(() => now++)

    let duringCleanup: number | undefined
    const backendDeleteObjectsDetailed = vi.fn((_bucket: string, keys: string[]) => {
      duringCleanup = Date.now()
      return Promise.resolve(keys.map((key) => ({ key, outcome: 'DELETED' as const })))
    })
    const db = {
      listObjects: vi.fn().mockResolvedValue([{ id: 'id-1', name: 'a.txt' }]),
      deleteObjects: vi.fn().mockResolvedValue([{ id: 'id-1', name: 'a.txt', version: 'v1' }]),
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

    try {
      await ObjectAdminDeleteAllBefore.handle(createJob() as never)

      const [, appliedAt] = sendWebhook.mock.calls[0] as [unknown, number]
      expect(appliedAt).toBeLessThan(duringCleanup!)
    } finally {
      dateNowSpy.mockRestore()
    }
  })

  it('reports objectsChanged by row, counting zero-byte objects and never delete markers', async () => {
    const db = {
      listObjects: vi
        .fn()
        .mockResolvedValue([{ id: 'id-1' }, { id: 'id-2' }, { id: 'id-3' }, { id: 'id-4' }]),
      deleteObjects: vi.fn().mockResolvedValue([
        { id: 'id-1', name: 'sized.txt', version: 'v1', metadata: { size: 7 } },
        { id: 'id-2', name: 'empty.txt', version: 'v1', metadata: { size: 0 } },
        { id: 'id-3', name: 'nosize.txt', version: 'v1', metadata: null },
        { id: 'id-4', name: 'sized.txt', version: 'v2', is_delete_marker: true, metadata: null },
      ]),
      destroyConnection: vi.fn(),
    }
    const storage = {
      db,
      backend: {
        deleteObjectsDetailed: vi.fn((_bucket: string, keys: string[]) =>
          Promise.resolve(keys.map((key) => ({ key, outcome: 'DELETED' as const })))
        ),
      },
      location: {
        getRootLocation: vi.fn(() => 'root-bucket'),
        getKeyLocation: vi.fn(() => 'tenant-id/bucket/key'),
      },
    }
    vi.spyOn(
      ObjectAdminDeleteAllBefore as unknown as { createStorage: () => Promise<unknown> },
      'createStorage'
    ).mockResolvedValue(storage)
    const sendWebhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)

    await ObjectAdminDeleteAllBefore.handle(createJob() as never)

    const payloads = sendWebhook.mock.calls.map(
      ([payload]) =>
        payload as { name: string; version: string; bytesChanged: number; objectsChanged: number }
    )
    expect(
      payloads.map(({ version, name, bytesChanged, objectsChanged }) => ({
        name,
        version,
        bytesChanged,
        objectsChanged,
      }))
    ).toEqual([
      { name: 'sized.txt', version: 'v1', bytesChanged: -7, objectsChanged: -1 },
      { name: 'empty.txt', version: 'v1', bytesChanged: 0, objectsChanged: -1 },
      { name: 'nosize.txt', version: 'v1', bytesChanged: 0, objectsChanged: -1 },
      { name: 'sized.txt', version: 'v2', bytesChanged: 0, objectsChanged: 0 },
    ])
  })

  it("captures a fresh applyTime per batch instead of reusing an earlier batch's", async () => {
    let now = 1_700_000_000_000
    const dateNowSpy = vi.spyOn(Date, 'now').mockImplementation(() => now++)

    const firstBatchObjects = Array.from({ length: MAX_OBJECTS_PER_DELETE_BATCH + 1 }, (_, i) => ({
      id: `id-${i}`,
      name: `a-${i}.txt`,
    }))
    const secondBatchObjects = [{ id: 'id-last', name: 'b.txt' }]
    const listObjects = vi
      .fn()
      .mockResolvedValueOnce(firstBatchObjects)
      .mockResolvedValueOnce(secondBatchObjects)
    const deleteObjects = vi.fn((_bucketId: string, ids: string[]) =>
      Promise.resolve(ids.map((id) => ({ id, name: `${id}.txt`, version: 'v1' })))
    )
    const backendDeleteObjectsDetailed = vi.fn((_bucket: string, keys: string[]) =>
      Promise.resolve(keys.map((key) => ({ key, outcome: 'DELETED' as const })))
    )
    const db = {
      listObjects,
      deleteObjects,
      destroyConnection: vi.fn(),
    }
    const storage = {
      db,
      backend: { deleteObjectsDetailed: backendDeleteObjectsDetailed },
      location: {
        getRootLocation: vi.fn(() => 'root-bucket'),
        getKeyLocation: vi.fn(() => 'tenant-id/bucket/key'),
      },
    }
    vi.spyOn(
      ObjectAdminDeleteAllBefore as unknown as { createStorage: () => Promise<unknown> },
      'createStorage'
    ).mockResolvedValue(storage)
    const sendWebhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)

    try {
      await ObjectAdminDeleteAllBefore.handle(createJob() as never)

      const calls = sendWebhook.mock.calls as [unknown, number][]
      const firstBatchApplyTimes = new Set(
        calls.slice(0, MAX_OBJECTS_PER_DELETE_BATCH).map((c) => c[1])
      )
      const [, secondBatchApplyTime] = calls[calls.length - 1]

      // Every object in the first batch shares that batch's one commit time...
      expect(firstBatchApplyTimes.size).toBe(1)
      const [firstBatchApplyTime] = firstBatchApplyTimes
      // ...and the second batch gets its own, later one rather than reusing it.
      expect(secondBatchApplyTime).toBeGreaterThan(firstBatchApplyTime)
    } finally {
      dateNowSpy.mockRestore()
    }
  })
})
