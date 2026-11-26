import fastify from 'fastify'
import { vi } from 'vitest'
import { signals } from '../../plugins/signals'

const mocks = vi.hoisted(() => {
  const wakeTenants = vi.fn()
  const logInfo = vi.fn()
  return {
    wakeTenants,
    logInfo,
    config: {
      isMultitenant: true,
      pgQueueEnable: true,
      storageLifecyclePaused: false,
      storageVersioningEnabled: false,
      adminApiKeys: 'test-admin-key',
      multitenantDatabaseQueryTimeout: 5000,
    },
  }
})

vi.mock('../../../config', () => ({ getConfig: () => mocks.config }))
vi.mock('@internal/database', () => ({
  multitenantPgExecutor: {},
  LifecycleTenantStorePg: class {
    wakeTenants = mocks.wakeTenants
  },
}))
vi.mock('@internal/monitoring', () => ({
  logSchema: {
    info: mocks.logInfo,
  },
}))

import routes, { LIFECYCLE_TENANT_RUN_MAX_BATCH_SIZE } from './lifecycle'

describe('admin lifecycle routes', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.wakeTenants.mockImplementation(async ({ tenantIds }: { tenantIds: string[] }) => ({
      wokenTenantIds: [...tenantIds],
      disabledTenantIds: [],
    }))
    mocks.config.isMultitenant = true
    mocks.config.pgQueueEnable = true
    mocks.config.storageLifecyclePaused = false
    mocks.config.storageVersioningEnabled = false
    mocks.config.multitenantDatabaseQueryTimeout = 5000
  })

  async function injectRequest(
    url: string,
    options: {
      apikey?: string
      headers?: Record<string, string>
      payload?: string | Record<string, unknown>
      sbReqId?: string
    } = {}
  ) {
    const app = fastify()
    app.decorateRequest('sbReqId', undefined)
    app.addHook('onRequest', (req, _reply, done) => {
      req.sbReqId = options.sbReqId
      done()
    })
    app.register(signals)
    app.register(routes, { prefix: '/tenants' })
    try {
      return await app.inject({
        method: 'POST',
        url,
        headers: {
          ...(options.apikey !== undefined
            ? { apikey: options.apikey }
            : { apikey: 'test-admin-key' }),
          ...options.headers,
        },
        payload: options.payload,
      })
    } finally {
      await app.close()
    }
  }

  describe('POST /tenants/:tenantId/lifecycle/run', () => {
    const request = (
      tenantId: string,
      options?: {
        apikey?: string
        headers?: Record<string, string>
        payload?: string
        sbReqId?: string
      }
    ) => injectRequest(`/tenants/${tenantId}/lifecycle/run`, options)

    it('wakes an eligible tenant and logs the audit event', async () => {
      mocks.config.storageVersioningEnabled = true

      const response = await request('tenant-a', { sbReqId: 'req-123' })

      expect(response.statusCode).toBe(202)
      expect(response.json()).toEqual({
        message: 'Lifecycle tenant run scheduled',
        tenantId: 'tenant-a',
      })
      expect(mocks.wakeTenants).toHaveBeenCalledWith({
        tenantIds: ['tenant-a'],
        storageVersioningEnabled: true,
        signal: expect.any(AbortSignal),
      })
      expect(mocks.logInfo).toHaveBeenCalledWith(
        expect.anything(),
        '[AdminLifecycle] Woke lifecycle tenant',
        expect.objectContaining({
          type: 'lifecycle-rule',
          tenantId: 'tenant-a',
          project: 'tenant-a',
          sbReqId: 'req-123',
        })
      )
    })

    it('accepts an empty application/json body', async () => {
      const response = await request('tenant-a', {
        headers: { 'content-type': 'application/json' },
        payload: '',
      })

      expect(response.statusCode).toBe(202)
      expect(mocks.wakeTenants).toHaveBeenCalledOnce()
    })

    it('returns 404 when the tenant does not exist', async () => {
      mocks.wakeTenants.mockResolvedValue({ wokenTenantIds: [], disabledTenantIds: [] })

      const response = await request('missing-tenant')

      expect(response.statusCode).toBe(404)
      expect(response.json()).toEqual({ error: 'Tenant not found' })
      expect(mocks.logInfo).not.toHaveBeenCalled()
    })

    it('returns 400 when lifecycle is not enabled for the tenant', async () => {
      mocks.wakeTenants.mockResolvedValue({
        wokenTenantIds: [],
        disabledTenantIds: ['disabled-tenant'],
      })

      const response = await request('disabled-tenant')

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({ error: 'Lifecycle is not enabled for tenant' })
      expect(mocks.logInfo).not.toHaveBeenCalled()
    })

    it('rejects requests outside multitenant mode', async () => {
      mocks.config.isMultitenant = false

      const response = await request('tenant-a')

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({
        error: 'This endpoint only supports multitenant mode',
      })
      expect(mocks.wakeTenants).not.toHaveBeenCalled()
    })

    it('requires an admin API key', async () => {
      const response = await request('tenant-a', { apikey: 'wrong-key' })

      expect(response.statusCode).toBe(401)
      expect(mocks.wakeTenants).not.toHaveBeenCalled()
    })

    it('rejects requests when the queue is disabled', async () => {
      mocks.config.pgQueueEnable = false

      const response = await request('tenant-a')

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({ error: 'Queue is not enabled' })
      expect(mocks.wakeTenants).not.toHaveBeenCalled()
    })

    it('rejects requests when lifecycle execution is paused', async () => {
      mocks.config.storageLifecyclePaused = true

      const response = await request('tenant-a')

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({ error: 'Lifecycle execution is paused' })
      expect(mocks.wakeTenants).not.toHaveBeenCalled()
    })

    it('rejects empty tenant ids and tenant ids containing NUL bytes', async () => {
      for (const badId of ['', 'tenant%00a']) {
        const response = await request(badId)

        expect(response.statusCode).toBe(400)
        expect(mocks.wakeTenants).not.toHaveBeenCalled()
      }
    })

    it('does not report success when waking fails', async () => {
      mocks.wakeTenants.mockRejectedValue(new Error('db unavailable'))

      const response = await request('tenant-a')

      expect(response.statusCode).toBe(500)
      expect(response.json()).not.toHaveProperty('tenantId')
    })
  })

  describe('POST /tenants/lifecycle/run', () => {
    const requestBatch = (
      payload?: string | Record<string, unknown>,
      options?: {
        apikey?: string
        headers?: Record<string, string>
        sbReqId?: string
      }
    ) => injectRequest('/tenants/lifecycle/run', { ...options, payload })

    it('wakes a deduplicated batch of eligible tenants and logs the audit event', async () => {
      const response = await requestBatch(
        { tenantIds: ['tenant-a', 'tenant-b', 'tenant-a'] },
        { sbReqId: 'req-batch-1' }
      )

      expect(response.statusCode).toBe(202)
      expect(response.json()).toEqual({
        message: 'Lifecycle tenant runs scheduled',
        scheduledTenantIds: ['tenant-a', 'tenant-b'],
        disabledTenantIds: [],
        missingTenantIds: [],
      })
      expect(mocks.wakeTenants).toHaveBeenCalledWith({
        tenantIds: ['tenant-a', 'tenant-b'],
        storageVersioningEnabled: false,
        signal: expect.any(AbortSignal),
      })
      expect(mocks.logInfo).toHaveBeenCalledWith(
        expect.anything(),
        '[AdminLifecycle] Woke lifecycle tenants',
        expect.objectContaining({
          type: 'lifecycle-rule',
          sbReqId: 'req-batch-1',
          metadata: JSON.stringify({
            requestedCount: 2,
            scheduledCount: 2,
            disabledCount: 0,
            missingCount: 0,
          }),
        })
      )
    })

    it('partitions scheduled, disabled, and missing tenantIds in request order', async () => {
      mocks.wakeTenants.mockResolvedValue({
        wokenTenantIds: ['tenant-c', 'tenant-a'],
        disabledTenantIds: ['disabled-2', 'disabled-1'],
      })

      const response = await requestBatch({
        tenantIds: ['tenant-a', 'disabled-1', 'missing-1', 'tenant-c', 'disabled-2', 'missing-2'],
      })

      expect(response.statusCode).toBe(202)
      expect(response.json()).toEqual({
        message: 'Lifecycle tenant runs scheduled',
        scheduledTenantIds: ['tenant-a', 'tenant-c'],
        disabledTenantIds: ['disabled-1', 'disabled-2'],
        missingTenantIds: ['missing-1', 'missing-2'],
      })
    })

    it('returns 400 when all matching tenants have lifecycle disabled', async () => {
      mocks.wakeTenants.mockResolvedValue({
        wokenTenantIds: [],
        disabledTenantIds: ['disabled-1'],
      })

      const response = await requestBatch({
        tenantIds: ['disabled-1', 'missing-1'],
      })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({
        error: 'Lifecycle is not enabled for any matching tenants',
        disabledTenantIds: ['disabled-1'],
        missingTenantIds: ['missing-1'],
      })
      expect(mocks.logInfo).not.toHaveBeenCalled()
    })

    it('returns 404 when no tenants in the batch exist', async () => {
      mocks.wakeTenants.mockResolvedValue({ wokenTenantIds: [], disabledTenantIds: [] })

      const response = await requestBatch({
        tenantIds: ['missing-1', 'missing-2'],
      })

      expect(response.statusCode).toBe(404)
      expect(response.json()).toEqual({
        error: 'No matching tenants found',
        disabledTenantIds: [],
        missingTenantIds: ['missing-1', 'missing-2'],
      })
      expect(mocks.logInfo).not.toHaveBeenCalled()
    })

    it('accepts up to 1000 tenantIds in a single batch', async () => {
      const tenantIds = Array.from(
        { length: LIFECYCLE_TENANT_RUN_MAX_BATCH_SIZE },
        (_, i) => `tenant-${i}`
      )

      const response = await requestBatch({ tenantIds })

      expect(response.statusCode).toBe(202)
      expect(response.json().scheduledTenantIds).toHaveLength(LIFECYCLE_TENANT_RUN_MAX_BATCH_SIZE)
      expect(response.json().disabledTenantIds).toEqual([])
      expect(response.json().missingTenantIds).toEqual([])
      expect(mocks.wakeTenants).toHaveBeenCalledOnce()
      expect(mocks.wakeTenants.mock.calls[0][0].tenantIds).toHaveLength(
        LIFECYCLE_TENANT_RUN_MAX_BATCH_SIZE
      )
    })

    it('rejects batches exceeding 1000 tenantIds', async () => {
      const tenantIds = Array.from(
        { length: LIFECYCLE_TENANT_RUN_MAX_BATCH_SIZE + 1 },
        (_, i) => `tenant-${i}`
      )

      const response = await requestBatch({ tenantIds })

      expect(response.statusCode).toBe(400)
      expect(mocks.wakeTenants).not.toHaveBeenCalled()
    })

    it.each([
      ['empty array', { tenantIds: [] }],
      ['missing tenantIds', {}],
      ['non-array tenantIds', { tenantIds: {} }],
      ['empty tenantId element', { tenantIds: [''] }],
      ['NUL byte element', { tenantIds: ['tenant\u0000a'] }],
      ['object element', { tenantIds: [{}] }],
      ['null element', { tenantIds: [null] }],
    ])('rejects invalid payload (%s)', async (_name, payload) => {
      const response = await requestBatch(payload)

      expect(response.statusCode).toBe(400)
      expect(response.json()).toMatchObject({
        error: 'Bad Request',
        message: expect.any(String),
      })
      expect(mocks.wakeTenants).not.toHaveBeenCalled()
    })

    it('rejects an empty application/json body', async () => {
      const response = await requestBatch('', {
        headers: { 'content-type': 'application/json' },
      })

      expect(response.statusCode).toBe(400)
      expect(mocks.wakeTenants).not.toHaveBeenCalled()
    })

    it('rejects requests outside multitenant mode', async () => {
      mocks.config.isMultitenant = false

      const response = await requestBatch({ tenantIds: ['tenant-a'] })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({
        error: 'This endpoint only supports multitenant mode',
      })
      expect(mocks.wakeTenants).not.toHaveBeenCalled()
    })

    it('requires an admin API key', async () => {
      const response = await requestBatch({ tenantIds: ['tenant-a'] }, { apikey: 'wrong-key' })

      expect(response.statusCode).toBe(401)
      expect(mocks.wakeTenants).not.toHaveBeenCalled()
    })

    it('rejects requests when the queue is disabled', async () => {
      mocks.config.pgQueueEnable = false

      const response = await requestBatch({ tenantIds: ['tenant-a'] })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({ error: 'Queue is not enabled' })
      expect(mocks.wakeTenants).not.toHaveBeenCalled()
    })

    it('rejects requests when lifecycle execution is paused', async () => {
      mocks.config.storageLifecyclePaused = true

      const response = await requestBatch({ tenantIds: ['tenant-a'] })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({ error: 'Lifecycle execution is paused' })
      expect(mocks.wakeTenants).not.toHaveBeenCalled()
    })

    it('does not report success when waking fails', async () => {
      mocks.wakeTenants.mockRejectedValue(new Error('db unavailable'))

      const response = await requestBatch({ tenantIds: ['tenant-a', 'tenant-b'] })

      expect(response.statusCode).toBe(500)
      expect(response.json()).not.toHaveProperty('scheduledTenantIds')
    })
  })
})
