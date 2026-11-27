import fastify from 'fastify'
import { onTestFinished } from 'vitest'
import { authSchema } from '../../schemas/auth'
import { errorSchema } from '../../schemas/error'
import moveObject from './moveObject'

describe('REST move response', () => {
  it('returns the destination object ID and key', async () => {
    const app = fastify()
    onTestFinished(() => app.close())
    app.addSchema(authSchema)
    app.addSchema(errorSchema)
    app.decorateRequest('storage')
    app.addHook('preHandler', async (request) => {
      request.storage = {
        from: () => ({
          moveObject: async () => ({
            destObject: {
              id: 'eaa8bdb5-2e00-4767-b5a9-d2502efe2196',
              name: 'folder/destination.txt',
              version: 'destination-version',
            },
          }),
        }),
      } as never
    })
    app.register(moveObject, { prefix: '/object' })

    const response = await app.inject({
      method: 'POST',
      url: '/object/move',
      headers: { authorization: 'Bearer route-test' },
      payload: {
        bucketId: 'source-bucket',
        sourceKey: 'source.txt',
        destinationBucket: 'destination-bucket',
        destinationKey: 'folder/destination.txt',
      },
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      message: 'Successfully moved',
      Id: 'eaa8bdb5-2e00-4767-b5a9-d2502efe2196',
      Key: 'folder/destination.txt',
      version: 'destination-version',
    })
  })
})
