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
  it('sends no webhook if the transaction fails to commit after its callback resolves', async () => {
    const deletedObj = { name: 'a.txt', version: 'v1', metadata: { size: 10 } }
    const backendDeleteObjects = vi.fn().mockResolvedValue(undefined)
    const trxDeleteObjects = vi.fn().mockResolvedValue([deletedObj])
    const db = {
      listObjects: vi.fn().mockResolvedValue([{ id: 'id-1', name: 'a.txt' }]),
      withTransaction: vi.fn(async (fn: (trx: unknown) => unknown) => {
        await fn({ deleteObjects: trxDeleteObjects })
        throw new Error('commit failed')
      }),
      destroyConnection: vi.fn(),
    }
    const storage = { db, backend: { deleteObjects: backendDeleteObjects } }
    vi.spyOn(
      ObjectAdminDeleteAllBefore as unknown as { createStorage: () => Promise<unknown> },
      'createStorage'
    ).mockResolvedValue(storage)
    const sendWebhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)

    await expect(ObjectAdminDeleteAllBefore.handle(createJob() as never)).rejects.toThrow(
      'commit failed'
    )

    // The backend cleanup for the deleted row's bytes did run (inside the transaction's callback)
    // real work happened, but the transaction never committed, so no webhook may fire for it.
    expect(trxDeleteObjects).toHaveBeenCalled()
    expect(backendDeleteObjects).toHaveBeenCalled()
    expect(sendWebhook).not.toHaveBeenCalled()
    expect(db.destroyConnection).toHaveBeenCalled()
  })
})
