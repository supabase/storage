import Fastify from 'fastify'
import { vi } from 'vitest'

type VectorPluginModule = typeof import('./vector')
type MockPgPoolOptions = {
  application_name?: string
  connectionString?: string
  max?: number
  min?: number
}
type MockPgPool = {
  options: MockPgPoolOptions
  on: ReturnType<typeof vi.fn>
  end: ReturnType<typeof vi.fn>
}

let createdPools: MockPgPool[] = []

async function loadVectorPlugin(
  configOverrides: Record<string, unknown> = {}
): Promise<VectorPluginModule> {
  vi.resetModules()
  mockPgModule()

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    databaseApplicationName: 'storage-test',
    isMultitenant: false,
    vectorBucketProvider: 'pgvector',
    vectorDatabaseCreate: false,
    vectorDatabaseURL: 'postgres://user:password@db.example.test:5432/storage_vectors',
    ...configOverrides,
  } as Parameters<typeof configModule.mergeConfig>[0])

  return import('./vector')
}

function getLatestPool(): MockPgPool {
  const pool = createdPools.at(-1)

  if (!pool) {
    throw new Error('Expected pg Pool to be created')
  }

  return pool
}

function mockPgModule(): void {
  createdPools = []

  vi.doMock('pg', () => {
    const types = {
      setTypeParser: vi.fn(),
    }

    class DatabaseError extends Error {}

    class MockPool implements MockPgPool {
      readonly options: MockPgPoolOptions
      on = vi.fn()
      end = vi.fn(async () => undefined)

      constructor(options: MockPgPoolOptions) {
        this.options = options
        createdPools.push(this)
      }
    }

    return {
      DatabaseError,
      Pool: MockPool,
      types,
      default: {
        DatabaseError,
        Pool: MockPool,
        types,
      },
    }
  })
}

describe('s3vector plugin', () => {
  afterEach(() => {
    vi.doUnmock('@internal/database')
    vi.doUnmock('@storage/protocols/vector')
    vi.doUnmock('pg')
    vi.resetModules()
  })

  it('installs an idle error handler on the single-tenant pgvector pool', async () => {
    const { s3vector } = await loadVectorPlugin()
    const app = Fastify()

    try {
      await app.register(s3vector)

      expect(getLatestPool().on).toHaveBeenCalledWith('error', expect.any(Function))
    } finally {
      await app.close()
    }
  })

  it.each([
    ['zero quotas', true, { maxBuckets: 0, maxIndexes: 0 }, 0, 0],
    ['zero bucket quota', true, { maxBuckets: 0 }, 0, 5],
    ['zero index quota', true, { maxIndexes: 0 }, 10, 0],
    ['positive quotas', true, { maxBuckets: 3, maxIndexes: 2 }, 3, 2],
    ['omitted quotas', true, {}, 10, 5],
    ['omitted vector settings', true, undefined, 10, 5],
    ['single-tenant defaults', false, { maxBuckets: 0, maxIndexes: 0 }, 10, 5],
  ] as const)(
    'passes %s to the vector manager from the request hook',
    async (_name, isMultitenant, tenantLimits, maxBucketCount, maxIndexCount) => {
      const getTenantConfigMock = vi.fn().mockResolvedValue({
        features: { vectorBuckets: tenantLimits },
      })
      const vectorManagerMock = vi.fn(function () {})
      vi.doMock('@internal/database', async (importOriginal) => ({
        ...(await importOriginal<typeof import('@internal/database')>()),
        getTenantConfig: getTenantConfigMock,
      }))
      vi.doMock('@storage/protocols/vector', async (importOriginal) => ({
        ...(await importOriginal<typeof import('@storage/protocols/vector')>()),
        VectorStoreManager: vectorManagerMock,
      }))
      const { s3vector } = await loadVectorPlugin({
        isMultitenant,
        vectorMaxBucketsCount: 10,
        vectorMaxIndexesCount: 5,
      })
      const app = Fastify()
      app.decorateRequest('tenantId', 'tenant-a')

      try {
        await app.register(s3vector)
        app.get('/probe', async () => ({ ok: true }))

        const response = await app.inject('/probe')

        expect(response.statusCode).toBe(200)
        expect(vectorManagerMock).toHaveBeenCalledExactlyOnceWith(
          expect.anything(),
          expect.anything(),
          expect.anything(),
          { tenantId: 'tenant-a', maxBucketCount, maxIndexCount }
        )
        if (isMultitenant) {
          expect(getTenantConfigMock).toHaveBeenCalledExactlyOnceWith('tenant-a')
        } else {
          expect(getTenantConfigMock).not.toHaveBeenCalled()
        }
      } finally {
        await app.close()
      }
    }
  )
})
