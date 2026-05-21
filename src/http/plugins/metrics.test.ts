import {
  httpRequestDuration,
  httpRequestSizeBytes,
  httpResponseSizeBytes,
} from '@internal/monitoring/metrics'
import Fastify from 'fastify'
import { httpMetrics } from './metrics'

describe('httpMetrics plugin', () => {
  it('records HTTP metric attributes without tenant id labels', async () => {
    const app = Fastify()
    const durationSpy = vi.spyOn(httpRequestDuration, 'record')
    const requestSizeSpy = vi.spyOn(httpRequestSizeBytes, 'add')
    const responseSizeSpy = vi.spyOn(httpResponseSizeBytes, 'add')

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
        status_code: '2xx',
      }

      expect(durationSpy).toHaveBeenCalledWith(expect.any(Number), expectedAttributes)
      expect(requestSizeSpy).toHaveBeenCalledWith(7, expectedAttributes)
      expect(responseSizeSpy).toHaveBeenCalledWith(2, expectedAttributes)
    } finally {
      durationSpy.mockRestore()
      requestSizeSpy.mockRestore()
      responseSizeSpy.mockRestore()
      await app.close()
    }
  })
})
