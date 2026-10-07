import { Readable } from 'node:stream'
import { ERRORS, ErrorCode } from '@internal/errors'
import { DBError } from '@storage/database/errors'
import Fastify from 'fastify'
import { DatabaseError } from 'pg'
import { setErrorHandler } from './error-handler'
import { xmlParser } from './plugins/xml'
import { s3ErrorHandler } from './routes/s3/error-handler'
import { errorSchema, sharedErrorResponseSchemas } from './schemas/error'

describe('setErrorHandler', () => {
  it.each([
    'REST',
    'S3',
  ])('serializes a %s stream error without object headers', async (protocol) => {
    const app = Fastify()
    if (protocol === 'S3') {
      await app.register(xmlParser)
      app.setErrorHandler(s3ErrorHandler)
    } else {
      setErrorHandler(app)
    }
    app.get('/download', (_request, reply) => {
      reply.raw.setHeader('X-Amz-Meta-Raw', 'raw-value')
      return reply
        .header('Content-Type', 'image/png')
        .header('Content-Encoding', 'gzip')
        .header('Content-Language', 'nl')
        .header('ETag', '"object-etag"')
        .header('Cache-Control', 'public, max-age=3600')
        .header('Expires', 'Thu, 01 Jan 2099 00:00:00 GMT')
        .header('Content-Range', 'bytes 0-9/100')
        .header('Content-Disposition', 'attachment; filename="object.png"')
        .header('Last-Modified', 'Thu, 01 Jan 2099 00:00:00 GMT')
        .header('Accept-Ranges', 'bytes')
        .header('X-Transformations', 'width:100')
        .header('X-Robots-Tag', 'noindex')
        .header('X-Amz-Meta-Owner', 'test-user')
        .header('X-Amz-Meta-Empty', '')
        .header('X-Amz-Request-Id', 'request-id')
        .header('Access-Control-Allow-Origin', 'https://example.test')
        .send(
          new Readable({
            read() {
              this.destroy(new Error('upstream stream failed'))
            },
          })
        )
    })

    try {
      const response = await app.inject({
        url: '/download',
        headers: { accept: protocol === 'S3' ? 'application/xml' : 'application/json' },
      })
      expect(response.statusCode).toBe(500)
      expect(response.headers['content-encoding']).toBeUndefined()
      expect(response.headers['content-language']).toBeUndefined()
      expect(response.headers.etag).toBeUndefined()
      expect(response.headers.expires).toBeUndefined()
      expect(response.headers['content-range']).toBeUndefined()
      expect(response.headers['content-disposition']).toBeUndefined()
      expect(response.headers['last-modified']).toBeUndefined()
      expect(response.headers['accept-ranges']).toBeUndefined()
      expect(response.headers['x-transformations']).toBeUndefined()
      expect(response.headers['x-robots-tag']).toBeUndefined()
      expect(response.headers['x-amz-meta-owner']).toBeUndefined()
      expect(response.headers['x-amz-meta-empty']).toBeUndefined()
      expect(response.headers['x-amz-meta-raw']).toBeUndefined()
      expect(response.headers['x-amz-request-id']).toBe('request-id')
      expect(response.headers['access-control-allow-origin']).toBe('https://example.test')
      expect(response.headers['cache-control']).toBe('no-store')
      if (protocol === 'REST') {
        expect(response.headers['content-type']).toContain('application/json')
        expect(response.json().code).toBe(ErrorCode.InternalError)
      } else {
        expect(response.headers['content-type']).toContain('application/xml')
        expect(response.body).toContain('<Code>InternalError</Code>')
      }
    } finally {
      await app.close()
    }
  })

  it.each([
    ['REST', 400],
    ['S3', 404],
  ] as const)('leaves Cache-Control unset on a %s error without staged headers', async (protocol, status) => {
    const app = Fastify()
    if (protocol === 'S3') {
      await app.register(xmlParser)
      app.setErrorHandler(s3ErrorHandler)
    } else {
      setErrorHandler(app)
    }
    app.get('/missing', async () => {
      throw ERRORS.NoSuchKey('missing.txt')
    })

    try {
      const response = await app.inject({
        url: '/missing',
        headers: { accept: protocol === 'S3' ? 'application/xml' : 'application/json' },
      })
      expect(response.statusCode).toBe(status)
      expect(response.headers['cache-control']).toBeUndefined()
    } finally {
      await app.close()
    }
  })

  it('preserves service codes through the shared 4xx response schema', async () => {
    const app = Fastify()
    app.addSchema(errorSchema)
    setErrorHandler(app)

    app.get(
      '/missing',
      {
        schema: {
          response: {
            '4xx': { $ref: 'errorSchema#' },
          },
        },
      },
      async () => {
        throw ERRORS.NoSuchKey('missing.txt')
      }
    )

    try {
      const response = await app.inject('/missing')

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({
        statusCode: '404',
        error: 'not_found',
        code: ErrorCode.NoSuchKey,
        message: 'Object not found',
      })
    } finally {
      await app.close()
    }
  })

  it('preserves service codes through the shared 5xx response schema', async () => {
    const app = Fastify()
    app.addSchema(errorSchema)
    setErrorHandler(app)

    app.get(
      '/internal-error',
      {
        schema: {
          response: sharedErrorResponseSchemas,
        },
      },
      async () => {
        throw ERRORS.InternalError()
      }
    )

    try {
      const response = await app.inject('/internal-error')

      expect(response.statusCode).toBe(500)
      expect(response.json()).toEqual({
        statusCode: '500',
        error: ErrorCode.InternalError,
        code: ErrorCode.InternalError,
        message: 'Internal server error',
      })
    } finally {
      await app.close()
    }
  })

  it('maps Fastify schema validation failures to InvalidRequest', async () => {
    const app = Fastify()
    setErrorHandler(app)

    app.post(
      '/validated',
      {
        schema: {
          body: {
            type: 'object',
            properties: {
              count: { type: 'number' },
            },
            required: ['count'],
            additionalProperties: false,
          },
        },
      },
      async () => ({ ok: true })
    )

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/validated',
        payload: { count: 'not-a-number' },
      })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toMatchObject({
        statusCode: '400',
        code: ErrorCode.InvalidRequest,
      })
    } finally {
      await app.close()
    }
  })

  it('uses the fallback status code in Fastify error payloads when statusCode is undefined', async () => {
    const app = Fastify()
    setErrorHandler(app)

    app.get('/undefined-status-code', async () => {
      const error = new Error('boom') as Error & { statusCode?: number }
      error.statusCode = undefined
      throw error
    })

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/undefined-status-code',
      })

      expect(response.statusCode).toBe(500)
      expect(response.json()).toMatchObject({
        statusCode: '500',
        code: ErrorCode.InternalError,
        message: 'boom',
      })
    } finally {
      await app.close()
    }
  })

  it('maps wrapped database slowdown errors to 429', async () => {
    const app = Fastify()
    setErrorHandler(app)

    app.get('/wrapped-slowdown', async () => {
      throw DBError.fromDBError(
        createPgError('08P01', 'no more connections allowed (max_client_conn)')
      )
    })

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/wrapped-slowdown',
      })

      expect(response.statusCode).toBe(429)
      expect(response.json()).toMatchObject({
        statusCode: '429',
        code: ErrorCode.SlowDown,
        error: 'too_many_connections',
      })
    } finally {
      await app.close()
    }
  })

  it('keeps wrapped non-slowdown connection errors as database errors', async () => {
    const app = Fastify()
    setErrorHandler(app)

    app.get('/wrapped-protocol-error', async () => {
      throw DBError.fromDBError(createPgError('08P01', 'received invalid response: 58'))
    })

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/wrapped-protocol-error',
      })

      expect(response.statusCode).toBe(500)
      expect(response.json()).toMatchObject({
        statusCode: '500',
        code: ErrorCode.DatabaseError,
        error: ErrorCode.DatabaseError,
      })
    } finally {
      await app.close()
    }
  })

  it('records the error before clearing staged headers', async () => {
    const app = Fastify()
    setErrorHandler(app)
    const error = ERRORS.InvalidRequest('rejected')
    let recorded: unknown
    app.addHook('onResponse', async (request) => {
      recorded = request.executionError
    })
    app.get('/fail', async (_request, reply) => {
      vi.spyOn(reply.raw, 'removeHeader').mockImplementation(() => {
        throw new Error('headers sent')
      })
      throw error
    })

    try {
      await app.inject('/fail')
      expect(recorded).toBe(error)
    } finally {
      await app.close()
    }
  })
})

function createPgError(code: string, message: string): DatabaseError {
  const error = new DatabaseError(message, message.length, 'error')
  error.code = code
  return error
}
