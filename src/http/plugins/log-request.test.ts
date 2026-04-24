import { Writable } from 'node:stream'
import Fastify from 'fastify'
import pino from 'pino'
import { logRequest } from './log-request'
import { requestContext } from './request-context'

function createApp(lines: string[]) {
  return Fastify({
    disableRequestLogging: true,
    loggerInstance: pino(
      { level: 'info' },
      new Writable({
        write(chunk, _encoding, callback) {
          lines.push(chunk.toString())
          callback()
        },
      })
    ),
  })
}

describe('log-request plugin', () => {
  let app: ReturnType<typeof createApp>
  let lines: string[]

  beforeEach(async () => {
    lines = []
    app = createApp(lines)

    app.decorateRequest('tenantId', 'tenant-a')
    await app.register(requestContext)
    await app.register(logRequest({}))

    app.get('/request-log', async () => {
      return { ok: true }
    })
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

  it('threads sbReqId into the request log data', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/request-log',
      headers: {
        'sb-request-id': 'sb-req-123',
      },
    })

    expect(response.statusCode).toBe(200)

    const requestLogLine = lines.find((line) => line.includes('"type":"request"'))

    expect(requestLogLine).toBeDefined()
    expect(requestLogLine).toContain('"sbReqId":"sb-req-123"')
    expect(requestLogLine).not.toContain('"request_id"')
  })

  it('threads tenant context into the request log data', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/request-log',
    })

    expect(response.statusCode).toBe(200)

    const requestLogLine = lines.find((line) => line.includes('"type":"request"'))

    expect(requestLogLine).toBeDefined()
    expect(requestLogLine).toContain('"tenantId":"tenant-a"')
    expect(requestLogLine).toContain('"project":"tenant-a"')
  })
})
