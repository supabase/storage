import Fastify from 'fastify'
import { vi } from 'vitest'
import { errorSchema } from '../../schemas/error'
import createIndexRoute from './create-index'

describe('CreateIndex route validation', () => {
  it('accepts a valid AWS S3 Vectors index name at the 63-character limit', async () => {
    const createVectorIndex = vi.fn().mockResolvedValue({ indexArn: 'arn:example' })
    const app = Fastify()
    app.addSchema(errorSchema)
    app.decorateRequest('s3Vector', undefined as never)
    app.addHook('onRequest', (request, _reply, done) => {
      request.s3Vector = { createVectorIndex } as never
      done()
    })
    await app.register(createIndexRoute)

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/CreateIndex',
        payload: {
          dataType: 'float32',
          dimension: 3,
          distanceMetric: 'cosine',
          indexName: 'a'.repeat(63),
          vectorBucketName: 'valid-vector-bucket',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(createVectorIndex).toHaveBeenCalledWith(
        expect.objectContaining({ indexName: 'a'.repeat(63) })
      )
    } finally {
      await app.close()
    }
  })
})
