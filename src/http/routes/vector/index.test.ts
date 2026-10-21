import { S3VectorsClient } from '@aws-sdk/client-s3vectors'
import { ErrorCode } from '@internal/errors'
import { HttpResponse } from '@smithy/protocol-http'
import { S3Vector } from '@storage/protocols/vector/adapter/s3-vector'
import type { FastifyInstance } from 'fastify'
import JSONBigint from 'json-bigint'
import { onTestFinished, vi } from 'vitest'
import { errorSchema } from '../../schemas/error'

const vectorCommandModules = [
  './create-bucket',
  './create-index',
  './delete-bucket',
  './delete-index',
  './delete-vectors',
  './get-bucket',
  './get-index',
  './get-vectors',
  './list-buckets',
  './list-indexes',
  './list-vectors',
  './put-vectors',
  './query-vectors',
]
const vectorProbeRouteModule = './create-bucket'

describe('vector routes', () => {
  afterEach(() => {
    vi.doUnmock('../../../config')
    vi.doUnmock('@internal/database')
    vi.doUnmock('../../plugins')
    vi.doUnmock('../../error-handler')
    vectorCommandModules.forEach((modulePath) => vi.doUnmock(modulePath))
    vi.resetModules()
  })

  it.each([
    {
      isMultitenant: true,
      shouldMountRoutes: true,
      shouldRegisterTenantGuard: true,
      vectorEnabled: true,
    },
    {
      isMultitenant: true,
      shouldMountRoutes: true,
      shouldRegisterTenantGuard: true,
      vectorEnabled: false,
    },
    {
      isMultitenant: false,
      shouldMountRoutes: true,
      shouldRegisterTenantGuard: false,
      vectorEnabled: true,
    },
    {
      isMultitenant: false,
      shouldMountRoutes: false,
      shouldRegisterTenantGuard: false,
      vectorEnabled: false,
    },
  ])(
    'mounts routes: $shouldMountRoutes and tenant guard: $shouldRegisterTenantGuard',
    async ({ isMultitenant, shouldMountRoutes, vectorEnabled, shouldRegisterTenantGuard }) => {
      const { app, registerJwtAuthMock, requireTenantFeatureMock } = await loadRoutes({
        isMultitenant,
        vectorEnabled,
      })

      try {
        if (shouldMountRoutes) {
          expect(registerJwtAuthMock).toHaveBeenCalled()
        } else {
          expect(registerJwtAuthMock).not.toHaveBeenCalled()
        }

        if (shouldRegisterTenantGuard) {
          expect(requireTenantFeatureMock).toHaveBeenCalledWith('vectorBuckets')
        } else {
          expect(requireTenantFeatureMock).not.toHaveBeenCalled()
        }
      } finally {
        await app.close()
      }
    }
  )

  it.each([
    {
      expectedBody: {
        error: 'FeatureNotEnabled',
        statusCode: '403',
        message: 'feature not enabled for this tenant',
        code: ErrorCode.FeatureNotEnabled,
      },
      expectedStatusCode: 403,
      hasFeature: false,
    },
    {
      expectedBody: { ok: true },
      expectedStatusCode: 200,
      hasFeature: true,
    },
  ])(
    'returns $expectedStatusCode from a vector route when tenant feature enabled is $hasFeature',
    async ({ expectedBody, expectedStatusCode, hasFeature }) => {
      const tenantHasFeatureMock = vi.fn().mockResolvedValue(hasFeature)
      const { app } = await loadRoutes({
        isMultitenant: true,
        vectorEnabled: true,
        useRealTenantFeatureGuard: true,
        tenantHasFeatureMock,
      })

      try {
        const response = await app.inject({
          method: 'GET',
          url: '/probe',
        })

        expect(response.statusCode).toBe(expectedStatusCode)
        expect(response.json()).toEqual(expectedBody)
        expect(tenantHasFeatureMock).toHaveBeenCalledWith('tenant-a', 'vectorBuckets')
      } finally {
        await app.close()
      }
    }
  )

  it('retains error response schema filtering', async () => {
    const { app } = await loadRoutes({ isMultitenant: false, vectorEnabled: true })
    onTestFinished(() => app.close())

    const response = await app.inject('/error-probe')

    expect(response.statusCode).toBe(400)
    expect(response.json()).toEqual({
      statusCode: '400',
      error: 'InvalidRequest',
      message: 'Bad request',
      code: ErrorCode.InvalidRequest,
    })
  })

  it('keeps the serializer scoped to vector routes', async () => {
    const { app } = await loadRoutes({ isMultitenant: false, vectorEnabled: true })
    onTestFinished(() => app.close())

    const response = await app.inject('/outside-vector')

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({ ok: true })
  })

  const metadata =
    '{"big":9007199254740993,"dec":0.1000000000000000055511151231257827,"exp":1e300,"frac":12345678901234567.5,"list":[-9007199254740993,1],"literal":{"type":"bigDecimal","string":"19.99","currency":"USD"},"safe":9007199254740991,"text":"x"}'
  const dataVector = (metadataField: string) =>
    `{"key":"k","data":{"float32":[1e20,0.100000001490116119384765625,1]}${metadataField}}`

  describe.each([
    ['/GetVectors', { keys: ['k'], returnData: true, returnMetadata: true }, dataVector],
    ['/ListVectors', { returnData: true, returnMetadata: true }, dataVector],
    [
      '/QueryVectors',
      {
        topK: 1,
        queryVector: { float32: [1] },
        returnMetadata: true,
        returnDistance: true,
      },
      (metadataField: string) =>
        `{"key":"k","distance":1e20${metadataField}},{"key":"j","distance":0.100000001490116119384765625}`,
    ],
  ] as const)('%s numeric responses', (url, input, makeVectors) => {
    async function readVector(vector: string, returnMetadata = true) {
      const client = new S3VectorsClient({
        region: 'us-east-1',
        credentials: { accessKeyId: 'a', secretAccessKey: 'b' },
        requestHandler: {
          handle: async () => ({
            response: new HttpResponse({
              statusCode: 200,
              headers: { 'content-type': 'application/json' },
              body: Buffer.from(`{"vectors":[${vector}],"distanceMetric":"euclidean"}`),
            }),
          }),
        },
      })
      const { app } = await loadRoutes({
        isMultitenant: false,
        vectorEnabled: true,
        vectorStore: new S3Vector(client),
      })
      onTestFinished(() => app.close())

      return app.inject({
        method: 'POST',
        url,
        payload: { vectorBucketName: 'bucket', indexName: 'index', ...input, returnMetadata },
      })
    }

    it.each([true, false])(
      'preserves number precision with returnMetadata=%s',
      async (returnMetadata) => {
        const vectors = makeVectors(returnMetadata ? `,"metadata":${metadata}` : '')
        const response = await readVector(vectors, returnMetadata)
        const json = JSONBigint({ alwaysParseAsBig: true })

        expect(response.statusCode).toBe(200)
        expect(response.headers['content-type']).toBe('application/json; charset=utf-8')
        expect(json.parse(response.body).vectors).toEqual(json.parse(`[${vectors}]`))
      }
    )

    it('keeps overflowing numbers numeric', async () => {
      const overflow = `1${'0'.repeat(309)}`
      const response = await readVector(
        `{"key":"k","metadata":{"positive":${overflow},"negative":-${overflow},"exp":1e309}}`
      )

      expect(response.statusCode).toBe(200)
      for (const [key, value] of [
        ['positive', overflow],
        ['negative', `-${overflow}`],
        ['exp', overflow],
      ]) {
        expect(response.body).toContain(`"${key}":${value}`)
      }
    })
  })
})

async function loadRoutes({
  isMultitenant,
  tenantHasFeatureMock = vi.fn(),
  useRealTenantFeatureGuard = false,
  vectorEnabled,
  vectorStore,
}: {
  isMultitenant: boolean
  tenantHasFeatureMock?: ReturnType<typeof vi.fn>
  useRealTenantFeatureGuard?: boolean
  vectorEnabled: boolean
  vectorStore?: S3Vector
}) {
  const registerJwtAuthMock = vi.fn()
  const requireTenantFeatureMock = vi.fn()
  const noopPlugin = async () => {}

  vi.doMock('../../../config', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../../config')>()

    return {
      ...actual,
      getConfig: (options?: Parameters<typeof actual.getConfig>[0]) => ({
        ...actual.getConfig(options),
        dbServiceRole: 'service_role',
        isMultitenant,
        vectorEnabled,
      }),
    }
  })
  vi.doMock('@internal/database', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@internal/database')>()

    return {
      ...actual,
      tenantHasFeature: tenantHasFeatureMock,
    }
  })

  vi.doMock('../../plugins', async () => {
    if (useRealTenantFeatureGuard) {
      const { requireTenantFeature } = await import('../../plugins/tenant-feature')

      return {
        dbSuperUser: noopPlugin,
        enforceJwtRole: noopPlugin,
        registerJwtAuth: registerJwtAuthMock,
        requireTenantFeature,
        s3vector: noopPlugin,
        signatureV4: noopPlugin,
      }
    }

    return {
      dbSuperUser: noopPlugin,
      enforceJwtRole: noopPlugin,
      registerJwtAuth: registerJwtAuthMock,
      requireTenantFeature: requireTenantFeatureMock.mockReturnValue(noopPlugin),
      s3vector: noopPlugin,
      signatureV4: noopPlugin,
    }
  })
  vi.doMock('../../error-handler', () => ({
    setErrorHandler: vi.fn(),
  }))
  vectorCommandModules.forEach((modulePath) => {
    if (
      vectorStore &&
      ['./get-vectors', './list-vectors', './query-vectors'].includes(modulePath)
    ) {
      return
    }
    vi.doMock(modulePath, () => ({
      default: modulePath === vectorProbeRouteModule ? probeRoute : noopPlugin,
    }))
  })

  const { default: fastify } = await import('fastify')
  const { default: routes } = await import('./index')
  const app = fastify()
  app.addSchema(errorSchema)
  app.get(
    '/outside-vector',
    { schema: { response: { 200: { type: 'object', properties: { ok: { type: 'boolean' } } } } } },
    async () => ({ ok: true, privateDetail: 'must be filtered' })
  )
  app.addHook('onRequest', (request, _reply, done) => {
    request.tenantId = 'tenant-a'
    if (vectorStore) {
      request.s3Vector = vectorStore as never
    }
    done()
  })
  await app.register(routes)
  await app.ready()

  return { app, registerJwtAuthMock, requireTenantFeatureMock }
}

async function probeRoute(fastify: FastifyInstance) {
  fastify.get(
    '/error-probe',
    { schema: { response: { '4xx': { $ref: 'errorSchema#' } } } },
    async (_request, reply) =>
      reply.code(400).send({
        statusCode: '400',
        error: 'InvalidRequest',
        message: 'Bad request',
        code: ErrorCode.InvalidRequest,
        privateDetail: 'must be filtered',
      })
  )
  fastify.get(
    '/probe',
    {
      schema: {
        response: {
          '4xx': { $ref: 'errorSchema#' },
        },
      },
    },
    async () => ({ ok: true })
  )
}
