import type { WirePayload } from '@internal/queue'
import type { PgBossTopicExtension } from '@supabase-labs/wave-adapter-pgboss'
import type { JobContext } from '@supabase-labs/wave-core'
import { vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  batch: vi.fn(),
  isMultitenant: true,
  info: vi.fn(),
  produce: vi.fn(),
}))
vi.mock('../../../config', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../config')>()
  return {
    ...actual,
    getConfig: () => ({ ...actual.getConfig(), isMultitenant: mocks.isMultitenant }),
  }
})
vi.mock('@internal/monitoring', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@internal/monitoring')>()
  return { ...actual, logSchema: { ...actual.logSchema, info: mocks.info } }
})
vi.mock('../queue', () => ({
  getStorageQueue: () => ({ produce: mocks.produce }),
}))
vi.mock('@storage/protocols/iceberg/catalog/reclaim-shard-slots', () => ({
  IcebergShardSlotReclaimer: class {
    runBatch = mocks.batch
  },
}))

import { storageTopics, TOPICS } from '../topics'
import {
  ReclaimIcebergShardSlots,
  ReclaimIcebergShardSlotsHandler,
  type ReclaimIcebergShardSlotsPayload,
} from './reclaim-shard-slots'

const payload: ReclaimIcebergShardSlotsPayload = {
  runId: 'run',
  dryRun: true,
  tenant: { ref: '', host: '' },
}
const data = new ReclaimIcebergShardSlots(payload).data

function makeCtx(): JobContext<WirePayload<ReclaimIcebergShardSlotsPayload>> {
  return {
    topic: TOPICS.reclaimIcebergShardSlots,
    group: TOPICS.reclaimIcebergShardSlots,
    message: { id: 'job-1', data, headers: {}, timestamp: 0, attempt: 1 },
    signal: new AbortController().signal,
    heartbeat: async () => {},
  }
}

const handler = new ReclaimIcebergShardSlotsHandler()

beforeEach(() => {
  vi.clearAllMocks()
  mocks.isMultitenant = true
  mocks.produce.mockResolvedValue(undefined)
})
it('chains a bounded batch with unchanged scope and dry-run mode', async () => {
  mocks.batch.mockResolvedValue({ scanned: 100, nextAfterReservationId: 'next-id' })
  await handler.handle(makeCtx())
  expect(mocks.batch).toHaveBeenCalledExactlyOnceWith(data)
  expect(mocks.produce).toHaveBeenCalledOnce()
  const next = mocks.produce.mock.calls[0][0]
  expect(next).toBeInstanceOf(ReclaimIcebergShardSlots)
  expect(next.data).toEqual({ ...data, afterReservationId: 'next-id' })
  expect(mocks.info).toHaveBeenCalledOnce()
  // The continuation's dedup key differs from the page that produced it.
  expect(new ReclaimIcebergShardSlots(payload).idempotencyKey).toBe('run:start')
  expect(next.idempotencyKey).toBe('run:next-id')
})
it('declares the v1 send options across the class, handler and topic', () => {
  // v1 getSendOptions: singletonKey → class idempotencyKey (asserted above);
  // retryLimit 3 / retryDelay 30 → handler retry policy; expireInMinutes 15 → topic expiry.
  expect(ReclaimIcebergShardSlots.allowSync).toBe(false)
  expect(handler.options).toEqual({
    prefetch: 1,
    retry: {
      maxAttempts: 4,
      backoffMs: 30_000,
      deadLetter: `${TOPICS.reclaimIcebergShardSlots}-dead-letter`,
    },
  })
  const topic = storageTopics.get<{ pgboss?: PgBossTopicExtension }>(
    TOPICS.reclaimIcebergShardSlots as string
  )
  expect(topic?.options?.pgboss).toEqual({ policy: 'exclusive', expireInSeconds: 15 * 60 })
})
it('finishes without enqueueing when no continuation remains', async () => {
  mocks.batch.mockResolvedValue({ scanned: 0 })
  await handler.handle(makeCtx())
  expect(mocks.produce).not.toHaveBeenCalled()
})
it('fails for retry if a continuation cannot be queued', async () => {
  mocks.batch.mockResolvedValue({ scanned: 100, nextAfterReservationId: 'next-id' })
  mocks.produce.mockRejectedValue(new Error('queue unavailable'))
  await expect(handler.handle(makeCtx())).rejects.toThrow('queue unavailable')
  expect(mocks.info).not.toHaveBeenCalled()
})
it('accepts a deduplicated continuation', async () => {
  // A dedup drop is invisible to produce: it resolves like any landed message.
  mocks.batch.mockResolvedValue({ scanned: 100, nextAfterReservationId: 'next-id' })
  await expect(handler.handle(makeCtx())).resolves.toBeUndefined()
})
it('rejects single-tenant execution', async () => {
  mocks.isMultitenant = false
  await expect(handler.handle(makeCtx())).rejects.toThrow('multitenant')
  expect(mocks.batch).not.toHaveBeenCalled()
})
