import * as monitoringMetrics from '@internal/monitoring/metrics'
import Fastify from 'fastify'
import { httpMetrics } from './metrics'

describe('httpMetrics plugin', () => {
  it('records HTTP metric attributes without tenant id labels', async () => {
    const app = Fastify()
    const durationSpy = vi.spyOn(monitoringMetrics.httpRequestDuration, 'record')
    const httpSizesSpy = vi.spyOn(monitoringMetrics, 'recordHttpSizes')

    app.decorateRequest('tenantId', 'tenant-a')
    await app.register(httpMetrics())
    app.post('/objects/:bucket', async (_request, reply) => {
      reply.header('content-length', '2')
      return 'ok'
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/objects/bucket-a',
        headers: {
          'content-length': '7',
          'content-type': 'text/plain',
        },
        payload: 'payload',
      })

      expect(response.statusCode).toBe(200)

      const expectedAttributes = {
        method: 'POST',
        operation: 'unknown',
        status_code: '200',
      }

      expect(durationSpy).toHaveBeenCalledWith(expect.any(Number), expectedAttributes)
      expect(httpSizesSpy).toHaveBeenCalledWith(7, 2, expectedAttributes)
    } finally {
      durationSpy.mockRestore()
      httpSizesSpy.mockRestore()
      await app.close()
    }
  })

  it('ignores malformed request content-length headers', async () => {
    const app = Fastify()
    const durationSpy = vi.spyOn(monitoringMetrics.httpRequestDuration, 'record')
    const httpSizesSpy = vi.spyOn(monitoringMetrics, 'recordHttpSizes')

    await app.register(httpMetrics())
    app.post('/objects/:bucket', async (_request, reply) => {
      reply.header('content-length', 'nope')
      return 'ok'
    })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/objects/bucket-a',
        headers: {
          'content-length': 'not-a-size',
          'content-type': 'text/plain',
        },
        payload: 'payload',
      })

      expect(response.statusCode).toBe(200)
      expect(durationSpy).toHaveBeenCalled()
      expect(httpSizesSpy).toHaveBeenCalledWith(
        undefined,
        2,
        expect.objectContaining({
          method: 'POST',
          operation: 'unknown',
          status_code: '200',
        })
      )
    } finally {
      durationSpy.mockRestore()
      httpSizesSpy.mockRestore()
      await app.close()
    }
  })
})
