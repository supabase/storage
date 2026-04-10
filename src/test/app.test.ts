'use strict'

import net, { AddressInfo } from 'node:net'
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
    })
  })
})
