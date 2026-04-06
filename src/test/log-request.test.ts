import Fastify, { FastifyInstance } from 'fastify'
import { logRequest } from '../http/plugins/log-request'

describe('log-request plugin', () => {
  let app: FastifyInstance

  beforeEach(async () => {
    app = Fastify()
    await app.register(logRequest({}))
  })

  afterEach(async () => {
    await app.close()
  })

  it('derives resources from route params and prefixes them', async () => {
    app.get('/bucket/:bucket/object/:name', async (request) => {
      return {
        resources: request.resources,
      }
    })

    const response = await app.inject({
      method: 'GET',
      url: '/bucket/demo/object/file.txt',
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      resources: ['/demo/file.txt'],
    })
  })

  it('prefers configured resources and normalizes missing leading slashes', async () => {
    app.get(
      '/bucket/:bucket',
      {
        config: {
          resources: () => ['bucket/demo', '/object/demo'],
        },
      },
      async (request) => {
        return {
          resources: request.resources,
        }
      }
    )

    const response = await app.inject({
      method: 'GET',
      url: '/bucket/demo',
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      resources: ['/bucket/demo', '/object/demo'],
    })
  })
})
