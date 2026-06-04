import { ErrorCode } from '@internal/errors'
import Fastify from 'fastify'
import { setErrorHandler } from './error-handler'

describe('setErrorHandler', () => {
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
})
