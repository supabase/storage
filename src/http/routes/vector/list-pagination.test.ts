import fastify from 'fastify'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { errorSchema } from '../../schemas/error'
import listBuckets from './list-buckets'
import listIndexes from './list-indexes'

describe('vector list pagination limits', () => {
  afterEach(() => vi.restoreAllMocks())

  it.each([
    ['/ListVectorBuckets', listBuckets, {}],
    ['/ListIndexes', listIndexes, { vectorBucketName: 'bucket' }],
  ] as const)('rejects a fractional maxResults on %s', async (url, route, fields) => {
    const app = fastify()
    app.addSchema(errorSchema)
    const listMock = vi.fn()
    app.addHook('onRequest', (request, _reply, done) => {
      Object.assign(request, {
        s3Vector: {
          listBuckets: listMock,
          listIndexes: listMock,
        },
      })
      done()
    })
    await app.register(route)

    try {
      const response = await app.inject({
        method: 'POST',
        url,
        payload: { ...fields, maxResults: 1.5 },
      })

      expect(response.statusCode).toBe(400)
      expect(listMock).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })
})
