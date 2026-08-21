import net, { AddressInfo } from 'node:net'
import { ErrorCode } from '@internal/errors'
import { FastifyInstance } from 'fastify'
import app from '../app'
import { getConfig } from '../config'

const { serviceKeyAsync } = getConfig()

type RawHttpResponse = {
  body: string
  statusLine: string
}

function sendRawRequest(port: number, rawRequest: string): Promise<RawHttpResponse> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: '127.0.0.1', port }, () => {
      socket.end(rawRequest)
    })

    let response = ''
    socket.setEncoding('utf8')
    socket.setTimeout(5000)
    socket.on('data', (chunk) => {
      response += chunk
    })
    socket.on('end', () => {
      const splitIndex = response.indexOf('\r\n\r\n')
      const rawHeaders = splitIndex === -1 ? response : response.slice(0, splitIndex)
      const body = splitIndex === -1 ? '' : response.slice(splitIndex + 4)

      resolve({
        body,
        statusLine: rawHeaders.split('\r\n')[0],
      })
    })
    socket.on('error', reject)
    socket.on('timeout', () => {
      socket.destroy()
      reject(new Error('Timed out waiting for raw HTTP response'))
    })
  })
}

describe('app request parsing', () => {
  let appInstance: FastifyInstance
  let port: number

  beforeEach(async () => {
    appInstance = app()
    await appInstance.listen({ host: '127.0.0.1', port: 0 })

    const address = appInstance.server.address()
    if (!address || typeof address === 'string') {
      throw new Error('Expected Fastify to listen on a TCP port')
    }

    port = (address as AddressInfo).port
  })

  afterEach(async () => {
    await appInstance.close()
  })

  test('returns invalid_mime_type for content-type headers with tabs on the wire', async () => {
    const response = await sendRawRequest(
      port,
      [
        'POST /bucket HTTP/1.1',
        'Host: 127.0.0.1',
        'Connection: close',
        `Authorization: Bearer ${await serviceKeyAsync}`,
        'Content-Type: image/\tpng',
        'Content-Length: 0',
        '',
        '',
      ].join('\r\n')
    )

    expect(response.statusLine).toBe('HTTP/1.1 400 Bad Request')
    expect(JSON.parse(response.body)).toEqual({
      statusCode: '415',
      error: 'invalid_mime_type',
      message: 'Invalid Content-Type header',
      code: ErrorCode.InvalidMimeType,
    })
  })

  test('returns schema-bound error codes for unmatched REST routes', async () => {
    const urls = [
      '/bucket/not-a-route/extra',
      '/object/not-a-route',
      '/render/image/not-a-route',
      '/cdn/not-a-route',
      '/vector/not-a-route',
    ]

    for (const url of urls) {
      const response = await appInstance.inject({
        method: 'GET',
        url,
      })

      expect(response.statusCode).toBe(404)
      expect(JSON.parse(response.body)).toEqual({
        statusCode: '404',
        error: 'Not Found',
        message: `Route GET:${url} not found`,
        code: ErrorCode.InvalidRequest,
      })
    }
  })

  test('leaves unmatched non-REST routes on their protocol-specific handlers', async () => {
    const s3Response = await appInstance.inject({
      method: 'GET',
      url: '/s3/not-a-route',
    })

    expect(s3Response.statusCode).toBe(403)
    expect(s3Response.headers['content-type']).toBe('application/xml; charset=utf-8')
    expect(s3Response.body).toContain('<Code>AccessDenied</Code>')

    for (const url of ['/iceberg/not-a-route', '/upload/resumable/not-a-route']) {
      const response = await appInstance.inject({
        method: 'GET',
        url,
      })

      expect(response.statusCode).toBe(404)
      expect(JSON.parse(response.body)).toEqual({
        message: `Route GET:${url} not found`,
        error: 'Not Found',
        statusCode: 404,
      })
    }
  })
})
