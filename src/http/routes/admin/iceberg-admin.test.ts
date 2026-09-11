import fastify from 'fastify'
import { vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  send: vi.fn(),
  config: { isMultitenant: true, pgQueueEnable: true, adminApiKeys: 'test-admin-key' },
}))
vi.mock('../../../config', () => ({ getConfig: () => mocks.config }))
vi.mock('@internal/database', () => ({ multitenantPgExecutor: {} }))
vi.mock('@internal/database/tenant', () => ({}))
vi.mock('@internal/queue', () => ({ SYSTEM_TENANT: { ref: '', host: '' } }))
vi.mock('@storage/events/iceberg', () => ({ DeleteIcebergResources: {} }))
vi.mock('@storage/events/iceberg/reclaim-shard-slots', () => ({
  ReclaimIcebergShardSlots: { send: mocks.send },
}))

import routes from './iceberg-admin'

beforeEach(() => {
  vi.clearAllMocks()
  mocks.send.mockResolvedValue('job-id')
  mocks.config.isMultitenant = true
  mocks.config.pgQueueEnable = true
})
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
  expect(body).toMatchObject({ dryRun: true, jobId: 'job-id' })
  expect(mocks.send).toHaveBeenCalledWith(
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
  expect(mocks.send).toHaveBeenCalledWith(
    expect.objectContaining({ dryRun: false, tenantId: 'tenant-a', shardId: '4' })
  )
})
it('requires an admin API key', async () => {
  expect((await request({}, 'wrong-key')).statusCode).toBe(401)
  expect(mocks.send).not.toHaveBeenCalled()
})
it.each([
  true,
  false,
])('accepts a resume cursor with dryRun=%s and unchanged scope', async (dryRun) => {
  const afterReservationId = '22222222-2222-4222-8222-222222222222'
  const response = await request({ dryRun, tenantId: 'tenant-a', shardId: '4', afterReservationId })
  expect(response.statusCode).toBe(202)
  expect(mocks.send).toHaveBeenCalledExactlyOnceWith(
    expect.objectContaining({ dryRun, tenantId: 'tenant-a', shardId: '4', afterReservationId })
  )
})
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
  expect(mocks.send).not.toHaveBeenCalled()
})
it.each(['isMultitenant', 'pgQueueEnable'] as const)('requires %s', async (flag) => {
  mocks.config[flag] = false
  expect((await request({})).statusCode).toBe(400)
  expect(mocks.send).not.toHaveBeenCalled()
})
it('does not report success when enqueueing is suppressed', async () => {
  mocks.send.mockResolvedValue(null)
  expect((await request({})).statusCode).toBe(409)
})
