import { ErrorCode } from '@internal/errors'
import fastifyPlugin from 'fastify-plugin'
import { vi } from 'vitest'

describe('S3 routes', () => {
  afterEach(() => {
    vi.doUnmock('../../../config')
    vi.doUnmock('@internal/database')
    vi.doUnmock('../../plugins')
    vi.resetModules()
  })

  it('returns an S3 XML 403 before downstream hooks when the tenant feature is disabled', async () => {
    const { app, downstreamHookMock, tenantHasFeatureMock } = await buildApp(false)

    try {
      const response = await app.inject({
        method: 'PUT',
        url: '/s3/videos/sbio-test.txt',
        headers: {
          'content-type': 'text/plain',
        },
        payload: 'test',
      })

      expect(response.statusCode).toBe(403)
      expect(response.headers['content-type']).toContain('application/xml')
      expect(response.payload).toBe(
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<Error>' +
          '<Resource>videos/sbio-test.txt</Resource>' +
          `<Code>${ErrorCode.FeatureNotEnabled}</Code>` +
          '<Message>feature not enabled for this tenant</Message>' +
          '</Error>'
      )
      expect(tenantHasFeatureMock).toHaveBeenCalledWith('tenant-a', 's3Protocol')
      expect(downstreamHookMock).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('continues to downstream S3 hooks when the tenant feature is enabled', async () => {
    const { app, downstreamHookMock, tenantHasFeatureMock } = await buildApp(true)

    try {
      const response = await app.inject({
        method: 'PUT',
        url: '/s3/videos/sbio-test.txt',
        headers: {
          'content-type': 'text/plain',
        },
        payload: 'test',
      })

      expect(response.statusCode).toBe(204)
      expect(tenantHasFeatureMock).toHaveBeenCalledWith('tenant-a', 's3Protocol')
      expect(downstreamHookMock).toHaveBeenCalledOnce()
    } finally {
      await app.close()
    }
  })

  it('returns an S3 XML error when a route rejects multipart form data', async () => {
    const { app } = await buildApp(true, { runHandler: true })

    try {
      const response = await app.inject({
        method: 'PUT',
        url: '/s3/videos/test.txt',
        headers: {
          'content-type': 'multipart/form-data; boundary=test-boundary',
        },
        payload: '--test-boundary--\r\n',
      })

      expect(response.statusCode).toBe(400)
      expect(response.headers['content-type']).toContain('application/xml')
      expect(response.payload).toBe(
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<Error>' +
          '<Resource>videos/test.txt</Resource>' +
          `<Code>${ErrorCode.InvalidRequest}</Code>` +
          '<Message>Multipart form data not supported</Message>' +
          '</Error>'
      )
    } finally {
      await app.close()
    }
  })
})

async function buildApp(
  hasFeature: boolean,
  options: { runHandler?: boolean; signedHeaders?: string[] } = {}
) {
  const tenantHasFeatureMock = vi.fn().mockResolvedValue(hasFeature)
  const downstreamHookMock = vi.fn()
  const noopPlugin = async () => {}
  const downstreamPlugin = fastifyPlugin(async (fastify) => {
    fastify.addHook('onRequest', async (request, reply) => {
      downstreamHookMock()
      // Stands in for the real signatureV4 plugin's request.signedHeaders decoration.
      if (options.signedHeaders) {
        request.signedHeaders = new Set(options.signedHeaders)
        return
      }
      if (!options.runHandler) {
        return reply.status(204).send()
      }
    })
  })

  vi.doMock('../../../config', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../../config')>()

    return {
      ...actual,
      getConfig: (options?: Parameters<typeof actual.getConfig>[0]) => ({
        ...actual.getConfig(options),
        isMultitenant: true,
        s3ProtocolEnabled: true,
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
    const [{ requireTenantFeature }, { xmlParser }] = await Promise.all([
      import('../../plugins/tenant-feature'),
      import('../../plugins/xml'),
    ])

    return {
      db: noopPlugin,
      detectS3IcebergBucket: noopPlugin,
      enforceJwtRole: noopPlugin,
      icebergRestCatalog: noopPlugin,
      requireTenantFeature,
      signatureV4: downstreamPlugin,
      storage: noopPlugin,
      xmlParser,
    }
  })

  const { default: fastify } = await import('fastify')
  const { default: routes } = await import('./index')
  const app = fastify()

  app.addHook('onRequest', (request, _reply, done) => {
    request.tenantId = 'tenant-a'
    done()
  })
  await app.register(routes, { prefix: '/s3' })
  await app.ready()

  return { app, downstreamHookMock, tenantHasFeatureMock }
}

describe('S3 signed-header route-selector gate', () => {
  afterEach(() => {
    vi.doUnmock('../../../config')
    vi.doUnmock('@internal/database')
    vi.doUnmock('../../plugins')
    vi.resetModules()
  })

  it('rejects an unsigned x-amz-copy-source header with 403 AccessDenied before matching any route', async () => {
    const { app } = await buildApp(true, { signedHeaders: ['host'] })

    try {
      const response = await app.inject({
        method: 'PUT',
        url: '/s3/videos/sbio-test.txt',
        headers: {
          'x-amz-copy-source': '/other-bucket/secret-key',
        },
      })

      expect(response.statusCode).toBe(403)
      expect(response.payload).toContain('<Code>AccessDenied</Code>')
      expect(response.payload).toContain('x-amz-copy-source')
    } finally {
      await app.close()
    }
  })

  it('does not reject when x-amz-copy-source is included in SignedHeaders', async () => {
    const { app } = await buildApp(true, { signedHeaders: ['host', 'x-amz-copy-source'] })
    const { S3ProtocolHandler } = await import('@storage/protocols/s3/s3-handler')
    const copyObject = vi.spyOn(S3ProtocolHandler.prototype, 'copyObject').mockResolvedValue({
      responseBody: { CopyObjectResult: { ETag: 'test-etag', LastModified: undefined } },
    })

    try {
      const response = await app.inject({
        method: 'PUT',
        url: '/s3/videos/sbio-test.txt',
        headers: {
          'x-amz-copy-source': '/other-bucket/secret-key',
        },
      })

      expect(copyObject).toHaveBeenCalledOnce()
      expect(response.statusCode).toBe(200)
    } finally {
      await app.close()
    }
  })

  it('rejects an unsigned x-amz-copy-source on UploadPartCopy the same way', async () => {
    const { app } = await buildApp(true, { signedHeaders: ['host'] })

    try {
      const response = await app.inject({
        method: 'PUT',
        url: '/s3/videos/sbio-test.txt?partNumber=1&uploadId=upload-id',
        headers: {
          'x-amz-copy-source': '/other-bucket/secret-key',
        },
      })

      expect(response.statusCode).toBe(403)
      expect(response.payload).toContain('<Code>AccessDenied</Code>')
    } finally {
      await app.close()
    }
  })
})

describe('hoistPresignedHeaders only trusts an actual presigned-query signature', () => {
  afterEach(() => {
    vi.resetModules()
  })

  it('does not hoist x-amz-* query params when the request is header-signed', async () => {
    const { hoistPresignedHeaders } = await import('./index')

    const req = {
      query: { 'X-Amz-Signature': 'forged', 'x-amz-meta-injected': 'attacker-controlled' },
      headers: {},
      isPresignedAuth: false,
      signedHeaders: new Set(['host']),
    } as unknown as Parameters<typeof hoistPresignedHeaders>[0]

    hoistPresignedHeaders(req)

    expect(req.headers['x-amz-meta-injected']).toBeUndefined()
    expect(req.signedHeaders?.has('x-amz-meta-injected')).toBe(false)
  })

  it('hoists x-amz-* query params into headers and SignedHeaders for an actual presigned-query request', async () => {
    const { hoistPresignedHeaders } = await import('./index')

    const req = {
      query: { 'X-Amz-Signature': 'real', 'x-amz-meta-injected': 'legitimate' },
      headers: {},
      isPresignedAuth: true,
      signedHeaders: new Set(['host']),
    } as unknown as Parameters<typeof hoistPresignedHeaders>[0]

    hoistPresignedHeaders(req)

    expect(req.headers['x-amz-meta-injected']).toBe('legitimate')
    expect(req.signedHeaders?.has('x-amz-meta-injected')).toBe(true)
  })

  it('does nothing when there is no X-Amz-Signature query param, presigned or not', async () => {
    const { hoistPresignedHeaders } = await import('./index')

    const req = {
      query: { 'x-amz-meta-injected': 'legitimate' },
      headers: {},
      isPresignedAuth: true,
      signedHeaders: new Set(['host']),
    } as unknown as Parameters<typeof hoistPresignedHeaders>[0]

    hoistPresignedHeaders(req)

    expect(req.headers['x-amz-meta-injected']).toBeUndefined()
    expect(req.signedHeaders?.has('x-amz-meta-injected')).toBe(false)
  })
})
