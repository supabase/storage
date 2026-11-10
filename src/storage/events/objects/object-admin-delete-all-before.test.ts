import type { WirePayload } from '@internal/queue'
import type { JobContext } from '@supabase-labs/wave-core'
import { describe, expect, it, vi } from 'vitest'
import { ObjectRemoved } from '../webhooks/lifecycle-events'
import {
  ObjectAdminDeleteAllBeforeHandler,
  type ObjectDeleteAllBeforeEvent,
} from './object-admin-delete-all-before'

const { createStorage } = vi.hoisted(() => ({ createStorage: vi.fn() }))

vi.mock('../base', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../base')>()),
  createStorage,
}))
vi.mock('../../database', () => ({
  StoragePgDB: class {},
}))
vi.mock('../../storage', () => ({
  Storage: class {},
}))

function createCtx(
  overrides: Partial<{ before: string; bucketId: string }> = {}
): JobContext<WirePayload<ObjectDeleteAllBeforeEvent>> {
  return {
    topic: 'object-admin-delete-all-before',
    group: 'object-admin-delete-all-before',
    message: {
      id: 'job-1',
      data: {
        before: overrides.before ?? new Date().toISOString(),
        bucketId: overrides.bucketId ?? 'bucket',
        tenant: { ref: 'tenant-id', host: 'localhost' },
        reqId: 'req-id',
        sbReqId: 'sb-req-id',
        region: 'local',
      },
      headers: {},
      timestamp: 0,
      attempt: 1,
    },
    signal: new AbortController().signal,
    heartbeat: async () => {},
  }
}

describe('ObjectAdminDeleteAllBeforeHandler.handle', () => {
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
    createStorage.mockResolvedValue(storage)
    const sendWebhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)

    await expect(new ObjectAdminDeleteAllBeforeHandler().handle(createCtx())).rejects.toThrow(
      'commit failed'
    )

    // The metadata delete never committed, so backend cleanup must never run
    // and no webhook may fire for content that was never actually removed.
    expect(backendDeleteObjectsDetailed).not.toHaveBeenCalled()
    expect(sendWebhook).not.toHaveBeenCalled()
    expect(db.destroyConnection).toHaveBeenCalled()
  })
})
