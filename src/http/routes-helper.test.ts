import fastify from 'fastify'
import { createDefaultSchema, createResponse } from './routes-helper'
import { authHeadersSchema, authSchema, errorSchema } from './schemas'

describe('testing Generic Routes Utils', () => {
  describe('creating generic responses [createResponse]', () => {
    test('create generic response with all values', () => {
      const response = {
        message: 'Invalid schema',
        statusCode: '400',
        error: 'Invalid schema error message',
      }

      expect(createResponse('Invalid schema', '400', 'Invalid schema error message')).toEqual(
        response
      )
    })

    test('create generic response without status', () => {
      const response = {
        message: 'Invalid schema',
        error: 'Invalid schema error message',
      }

      expect(createResponse('Invalid schema', undefined, 'Invalid schema error message')).toEqual(
        response
      )
    })

    test('create generic response without error', () => {
      const response = {
        message: 'Invalid schema',
        statusCode: '400',
      }

      expect(createResponse('Invalid schema', '400')).toEqual(response)
    })

    test('create generic response only message', () => {
      const response = {
        message: 'Invalid schema',
      }

      expect(createResponse('Invalid schema')).toEqual(response)
    })
  })

  describe('creating generic schema [createDefaultSchema]', () => {
    test('create generic schema without additional properties', () => {
      const successResponseSchema = { generic: 'example', description: 'Successful response' }
      const response = {
        headers: authHeadersSchema,
        response: {
          200: successResponseSchema,
          '4xx': { $ref: 'errorSchema#', description: 'Error response' },
          '5xx': { $ref: 'errorSchema#', description: 'Error response' },
        },
      }

      expect(createDefaultSchema(successResponseSchema, {})).toEqual(response)
    })

    test('create generic schema with additional properties', () => {
      const successResponseSchema = { generic: 'example', description: 'Successful response' }
      const additionalProperties = { generic: 'example' }
      const response = {
        headers: authHeadersSchema,
        response: {
          200: successResponseSchema,
          '4xx': { $ref: 'errorSchema#', description: 'Error response' },
          '5xx': { $ref: 'errorSchema#', description: 'Error response' },
        },
        ...additionalProperties,
      }

      expect(createDefaultSchema(successResponseSchema, additionalProperties)).toEqual(response)
    })

    test('requires case-insensitive authorization headers without shared schema warnings', async () => {
      const emitWarning = vi.spyOn(process, 'emitWarning')
      const app = fastify()
      app.addSchema(authSchema)
      app.addSchema(errorSchema)

      for (const url of ['/first', '/second']) {
        app.get(url, {
          schema: createDefaultSchema({ type: 'string' }, {}),
          handler: async (request) => request.headers.authorization,
        })
      }

      try {
        for (const url of ['/first', '/second']) {
          const missingHeader = await app.inject({ method: 'GET', url })
          expect(missingHeader.statusCode).toBe(400)
          expect(missingHeader.json().message).toContain("required property 'authorization'")

          for (const header of ['authorization', 'Authorization', 'AUTHORIZATION']) {
            const response = await app.inject({
              method: 'GET',
              url,
              headers: { [header]: 'Bearer test' },
            })
            expect(response.statusCode).toBe(200)
            expect(response.body).toBe('Bearer test')
          }
        }
        expect(emitWarning).not.toHaveBeenCalledWith(
          expect.any(String),
          'FastifySecurity',
          'FSTSEC002'
        )
      } finally {
        await app.close()
        emitWarning.mockRestore()
      }
    })
  })
})
