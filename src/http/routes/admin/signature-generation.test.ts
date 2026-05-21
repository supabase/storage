import { tenantHasMigrations } from '@internal/database/migrations'
import { vi } from 'vitest'

const { mockGenerateObjectSignaturesSend, mockTenantHasMigrations } = vi.hoisted(() => ({
  mockGenerateObjectSignaturesSend: vi.fn(),
  mockTenantHasMigrations: vi.fn(),
}))

vi.mock('@internal/database/migrations', () => ({
  tenantHasMigrations: mockTenantHasMigrations,
}))

vi.mock('@storage/events', () => ({
  GenerateObjectSignatures: {
    send: mockGenerateObjectSignaturesSend,
  },
}))

describe('admin signature generation routes', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.resetModules()
    mockTenantHasMigrations.mockResolvedValue(true)
  })

  async function createApp(config: { pgQueueEnable: boolean }) {
    const { mergeConfig } = await import('../../../config')
    mergeConfig({
      adminApiKeys: 'test-admin-key',
      ...config,
    })

    const fastify = (await import('fastify')).default
    const { default: routes } = await import('./signature-generation')

    const app = fastify()
    app.decorateRequest('sbReqId', undefined)
    app.addHook('onRequest', (request, _reply, done) => {
      request.sbReqId =
        typeof request.headers['sb-request-id'] === 'string'
          ? request.headers['sb-request-id']
          : undefined
      done()
    })
    app.register(routes, { prefix: '/tenants' })
    return app
  }

  it('rejects signature generation requests when the queue is disabled', async () => {
    const app = await createApp({
      pgQueueEnable: false,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
        headers: {
          apikey: 'test-admin-key',
        },
      })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({ message: 'Queue is not enabled' })
      expect(mockGenerateObjectSignaturesSend).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('requires an admin api key', async () => {
    const app = await createApp({
      pgQueueEnable: true,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
      })

      expect(response.statusCode).toBe(401)
      expect(mockGenerateObjectSignaturesSend).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('rejects invalid admin api keys', async () => {
    const app = await createApp({
      pgQueueEnable: true,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
        headers: {
          apikey: 'wrong-admin-key',
        },
      })

      expect(response.statusCode).toBe(401)
      expect(mockGenerateObjectSignaturesSend).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('requires a bucket id when object names are provided', async () => {
    const app = await createApp({
      pgQueueEnable: true,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
        headers: {
          apikey: 'test-admin-key',
          'content-type': 'application/json',
        },
        payload: {
          objectNames: ['a.txt'],
        },
      })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({ message: 'bucketId is required when objectNames is set' })
      expect(mockGenerateObjectSignaturesSend).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('rejects empty bucket ids', async () => {
    const app = await createApp({
      pgQueueEnable: true,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
        headers: {
          apikey: 'test-admin-key',
          'content-type': 'application/json',
        },
        payload: {
          bucketId: '',
        },
      })

      expect(response.statusCode).toBe(400)
      expect(mockGenerateObjectSignaturesSend).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('rejects empty object names', async () => {
    const app = await createApp({
      pgQueueEnable: true,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
        headers: {
          apikey: 'test-admin-key',
          'content-type': 'application/json',
        },
        payload: {
          bucketId: 'bucket-a',
          objectNames: [''],
        },
      })

      expect(response.statusCode).toBe(400)
      expect(mockGenerateObjectSignaturesSend).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('rejects too many object names', async () => {
    const app = await createApp({
      pgQueueEnable: true,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
        headers: {
          apikey: 'test-admin-key',
          'content-type': 'application/json',
        },
        payload: {
          bucketId: 'bucket-a',
          objectNames: Array.from({ length: 1001 }, (_, index) => `object-${index}`),
        },
      })

      expect(response.statusCode).toBe(400)
      expect(mockGenerateObjectSignaturesSend).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('rejects requests before the tenant has the object signature migration', async () => {
    vi.mocked(tenantHasMigrations).mockResolvedValue(false)

    const app = await createApp({
      pgQueueEnable: true,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
        headers: {
          apikey: 'test-admin-key',
        },
      })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({
        message:
          'Tenant migrations must include add-objects-signature before generating signatures',
      })
      expect(tenantHasMigrations).toHaveBeenCalledWith('project-a', 'add-objects-signature')
      expect(mockGenerateObjectSignaturesSend).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('rejects broad signature generation before the tenant has the signature index migration', async () => {
    vi.mocked(tenantHasMigrations).mockImplementation(async (_tenantId, migration) => {
      return migration === 'add-objects-signature'
    })

    const app = await createApp({
      pgQueueEnable: true,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
        headers: {
          apikey: 'test-admin-key',
          'content-type': 'application/json',
        },
        payload: {
          bucketId: 'bucket-a',
        },
      })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({
        message:
          'Tenant migrations must include add-objects-signature-index before broad signature generation',
      })
      expect(tenantHasMigrations).toHaveBeenCalledWith('project-a', 'add-objects-signature')
      expect(tenantHasMigrations).toHaveBeenCalledWith('project-a', 'add-objects-signature-index')
      expect(mockGenerateObjectSignaturesSend).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('allows scoped object signature generation before the signature index migration', async () => {
    vi.mocked(tenantHasMigrations).mockImplementation(async (_tenantId, migration) => {
      return migration === 'add-objects-signature'
    })
    mockGenerateObjectSignaturesSend.mockResolvedValue('job-id-scoped-no-index')

    const app = await createApp({
      pgQueueEnable: true,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
        headers: {
          apikey: 'test-admin-key',
          'content-type': 'application/json',
        },
        payload: {
          bucketId: 'bucket-a',
          objectNames: ['a.txt'],
        },
      })

      expect(response.statusCode).toBe(200)
      expect(mockGenerateObjectSignaturesSend).toHaveBeenCalledWith({
        tenant: { ref: 'project-a', host: '' },
        bucketId: 'bucket-a',
        objectNames: ['a.txt'],
        force: false,
        reqId: expect.any(String),
        sbReqId: undefined,
      })
      expect(tenantHasMigrations).toHaveBeenCalledWith('project-a', 'add-objects-signature')
      expect(tenantHasMigrations).not.toHaveBeenCalledWith(
        'project-a',
        'add-objects-signature-index'
      )
    } finally {
      await app.close()
    }
  })

  it('schedules tenant-wide signature generation from an empty json body', async () => {
    mockGenerateObjectSignaturesSend.mockResolvedValue('job-id-empty-body')

    const app = await createApp({
      pgQueueEnable: true,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
        headers: {
          apikey: 'test-admin-key',
          'content-type': 'application/json',
        },
        payload: '',
      })

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({
        message: 'Object signature generation scheduled',
        jobId: 'job-id-empty-body',
      })
      expect(mockGenerateObjectSignaturesSend).toHaveBeenCalledWith({
        tenant: { ref: 'project-a', host: '' },
        bucketId: undefined,
        objectNames: undefined,
        force: false,
        reqId: expect.any(String),
        sbReqId: undefined,
      })
    } finally {
      await app.close()
    }
  })

  it('schedules a scoped object signature generation job', async () => {
    mockGenerateObjectSignaturesSend.mockResolvedValue('job-id-1')

    const app = await createApp({
      pgQueueEnable: true,
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/tenants/project-a/storage/generate-signatures',
        headers: {
          apikey: 'test-admin-key',
          'content-type': 'application/json',
          'sb-request-id': 'sb-req-123',
        },
        payload: {
          bucketId: 'bucket-a',
          objectNames: ['a.txt', 'folder/b.txt'],
          force: true,
        },
      })

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({
        message: 'Object signature generation scheduled',
        jobId: 'job-id-1',
      })
      expect(mockGenerateObjectSignaturesSend).toHaveBeenCalledWith({
        tenant: { ref: 'project-a', host: '' },
        bucketId: 'bucket-a',
        objectNames: ['a.txt', 'folder/b.txt'],
        force: true,
        reqId: expect.any(String),
        sbReqId: 'sb-req-123',
      })
    } finally {
      await app.close()
    }
  })
})
