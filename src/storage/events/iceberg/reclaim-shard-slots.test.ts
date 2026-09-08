import type { Job } from 'pg-boss'
import { vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  batch: vi.fn(),
  config: {
    isMultitenant: true,
    icebergCatalogUrl: 'http://catalog',
    icebergCatalogAuthType: 'none',
  },
  info: vi.fn(),
}))
vi.mock('../../../config', () => ({ getConfig: () => mocks.config }))
vi.mock('@internal/database', () => ({ multitenantPgExecutor: {} }))
vi.mock('@internal/monitoring', () => ({ logger: {}, logSchema: { info: mocks.info } }))
vi.mock('@internal/queue', () => ({
  Event: class {
    static send = vi.fn()
  },
}))
vi.mock('@storage/protocols/iceberg/catalog', () => ({
  getCatalogAuthStrategy: vi.fn(),
  RestCatalogClient: class {},
}))
vi.mock('@storage/protocols/iceberg/catalog/reclaim-shard-slots', () => ({
  IcebergShardSlotReclaimer: class {
    runBatch = mocks.batch
  },
}))

import {
  ReclaimIcebergShardSlots,
  type ReclaimIcebergShardSlotsPayload,
} from './reclaim-shard-slots'

const payload: ReclaimIcebergShardSlotsPayload = {
  runId: 'run',
  dryRun: true,
  tenant: { ref: '', host: '' },
}
const job = { data: payload } as Job<ReclaimIcebergShardSlotsPayload>

beforeEach(() => {
  vi.clearAllMocks()
  mocks.config.isMultitenant = true
})
it('chains a bounded batch with unchanged scope and dry-run mode', async () => {
  mocks.batch.mockResolvedValue({ scanned: 100, nextAfterReservationId: 'next-id' })
  const send = vi.spyOn(ReclaimIcebergShardSlots, 'send').mockResolvedValue('next-job')
  await ReclaimIcebergShardSlots.handle(job)
  expect(send).toHaveBeenCalledExactlyOnceWith({ ...payload, afterReservationId: 'next-id' })
  expect(mocks.info).toHaveBeenCalledOnce()
  expect(ReclaimIcebergShardSlots.getSendOptions(payload).singletonKey).not.toBe(
    ReclaimIcebergShardSlots.getSendOptions({ ...payload, afterReservationId: 'next-id' })
      .singletonKey
  )
})
it('finishes without enqueueing when no continuation remains', async () => {
  mocks.batch.mockResolvedValue({ scanned: 0 })
  const send = vi.spyOn(ReclaimIcebergShardSlots, 'send')
  await ReclaimIcebergShardSlots.handle(job)
  expect(send).not.toHaveBeenCalled()
})
it('fails for retry if a continuation cannot be queued', async () => {
  mocks.batch.mockResolvedValue({ scanned: 100, nextAfterReservationId: 'next-id' })
  vi.spyOn(ReclaimIcebergShardSlots, 'send').mockRejectedValue(new Error('queue unavailable'))
  await expect(ReclaimIcebergShardSlots.handle(job)).rejects.toThrow('queue unavailable')
})
it('accepts a deduplicated continuation', async () => {
  mocks.batch.mockResolvedValue({ scanned: 100, nextAfterReservationId: 'next-id' })
  vi.spyOn(ReclaimIcebergShardSlots, 'send').mockResolvedValue(null)
  await expect(ReclaimIcebergShardSlots.handle(job)).resolves.toBeUndefined()
})
it('rejects single-tenant execution', async () => {
  mocks.config.isMultitenant = false
  await expect(ReclaimIcebergShardSlots.handle(job)).rejects.toThrow('multitenant')
  expect(mocks.batch).not.toHaveBeenCalled()
})
