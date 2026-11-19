import fastify from 'fastify'
import { vi } from 'vitest'

const mocks = vi.hoisted(() => {
  const produce = vi.fn()
  return {
    produce,
    // Stand-in message class: the route only calls its `send` static, which (like the real one)
    // builds the message and hands it to produce.
    ReclaimIcebergShardSlots: class {
      constructor(readonly data: Record<string, unknown>) {}
      static send(payload: Record<string, unknown>): Promise<void> {
        return produce(new this(payload))
      }
    },
    DeleteIcebergResources: class {
      constructor(readonly data: Record<string, unknown>) {}
      static batchSend(payloads: Record<string, unknown>[]): Promise<void> {
        return produce(payloads.map((payload) => new this(payload)))
      }
    },
    config: { isMultitenant: true, pgQueueEnable: true, adminApiKeys: 'test-admin-key' },
  }
})
vi.mock('../../../config', () => ({ getConfig: () => mocks.config }))
vi.mock('@internal/database', () => ({ multitenantPgExecutor: {} }))
vi.mock('@internal/database/tenant', () => ({}))
vi.mock('@internal/queue', () => ({ SYSTEM_TENANT: { ref: '', host: '' } }))
vi.mock('@storage/events', () => ({
  DeleteIcebergResources: mocks.DeleteIcebergResources,
  ReclaimIcebergShardSlots: mocks.ReclaimIcebergShardSlots,
}))

import routes from './iceberg-admin'

beforeEach(() => {
  vi.clearAllMocks()
  mocks.produce.mockResolvedValue(undefined)
  mocks.config.isMultitenant = true
  mocks.config.pgQueueEnable = true
})
/** The payload of the one `ReclaimIcebergShardSlots` message produced (v1: `send`'s argument). */
function producedPayload() {
  expect(mocks.produce).toHaveBeenCalledOnce()
  const [message] = mocks.produce.mock.calls[0]
  expect(message).toBeInstanceOf(mocks.ReclaimIcebergShardSlots)
  return message.data
}
async function request(payload: Record<string, unknown>, apikey = 'test-admin-key') {
  const app = fastify()
  app.register(routes, { prefix: '/tenants' })
  try {
    return await app.inject({
      method: 'POST',
      url: '/tenants/iceberg/reclaim-shard-slots',
      headers: { apikey },
      payload,
    })
  } finally {
    await app.close()
  }
}

it('defaults to a regional dry-run and returns a correlation ID', async () => {
  const response = await request({})
  expect(response.statusCode).toBe(202)
  const body = response.json()
  // v2 produce resolves to nothing: the response carries the run's correlation ID, no job ID.
  expect(body).toEqual({ runId: expect.any(String), dryRun: true })
  expect(producedPayload()).toEqual(
    expect.objectContaining({
      runId: body.runId,
      dryRun: true,
      tenantId: undefined,
      shardId: undefined,
      afterReservationId: undefined,
    })
  )
})
it('accepts explicit reclamation with tenant and shard filters', async () => {
  const response = await request({ dryRun: false, tenantId: 'tenant-a', shardId: '4' })
  expect(response.statusCode).toBe(202)
  expect(producedPayload()).toEqual(
    expect.objectContaining({ dryRun: false, tenantId: 'tenant-a', shardId: '4' })
  )
})
it('requires an admin API key', async () => {
  expect((await request({}, 'wrong-key')).statusCode).toBe(401)
  expect(mocks.produce).not.toHaveBeenCalled()
})
it.each([true, false])(
  'accepts a resume cursor with dryRun=%s and unchanged scope',
  async (dryRun) => {
    const afterReservationId = '22222222-2222-4222-8222-222222222222'
    const response = await request({
      dryRun,
      tenantId: 'tenant-a',
      shardId: '4',
      afterReservationId,
    })
    expect(response.statusCode).toBe(202)
    expect(producedPayload()).toEqual(
      expect.objectContaining({ dryRun, tenantId: 'tenant-a', shardId: '4', afterReservationId })
    )
  }
)
it.each([
  { shardId: '-1' },
  { shardId: 'bad' },
  { tenantId: '' },
  { dryRun: 'invalid' },
  { afterReservationId: '' },
  { afterReservationId: 'not-a-uuid' },
  { afterReservationId: '22222222-2222-4222-8222-22222222222z' },
  { afterReservationId: 'urn:uuid:22222222-2222-4222-8222-222222222222' },
])('rejects invalid scope or mode %j', async (payload) => {
  expect((await request(payload)).statusCode).toBe(400)
  expect(mocks.produce).not.toHaveBeenCalled()
})
it.each(['isMultitenant', 'pgQueueEnable'] as const)('requires %s', async (flag) => {
  mocks.config[flag] = false
  expect((await request({})).statusCode).toBe(400)
  expect(mocks.produce).not.toHaveBeenCalled()
})
// v1 answered 409 when `send` resolved to no job id (suppressed/deduplicated). A v2 produce
// cannot observe that, so the only non-success left is a produce failure — which propagates.
it('does not report success when enqueueing fails', async () => {
  mocks.produce.mockRejectedValue(new Error('queue unavailable'))
  const response = await request({})
  expect(response.statusCode).toBe(500)
  expect(response.json()).not.toHaveProperty('runId')
})
