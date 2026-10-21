import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { AddressInfo, Socket } from 'node:net'
import { ErrorCode, type StorageBackendError } from '@internal/errors'
import JSONBigint from 'json-bigint'
import { afterEach, describe, expect, it } from 'vitest'
import { IcebergError, IcebergErrorType } from './errors'
import { BearerTokenAuth, RestCatalogClient, SignV4Auth } from './rest-catalog-client'

interface TestServer {
  close(): Promise<void>
  url: string
}

type RequestRecord = {
  body: string
  headers: IncomingMessage['headers']
  method?: string
  rawHeaders: string[]
  url?: string
}

const servers: TestServer[] = []

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => server.close()))
})

describe('RestCatalogClient HTTP contract', () => {
  it.each(['listTables', 'dropTable', 'dropNamespace'] as const)(
    'applies client-wide structured-error confirmation to %s',
    async (operation) => {
      const missingNamespace = JSON.stringify({
        error: { message: 'gone', type: IcebergErrorType.NoSuchNamespaceException, code: 404 },
      })
      let reply = { status: 404, body: 'not found', contentType: 'text/plain' }
      const server = await startServer((_req, res) => {
        res.writeHead(reply.status, { 'Content-Type': reply.contentType }).end(reply.body)
      })
      const client = new RestCatalogClient({
        catalogUrl: `${server.url}/v1`,
        auth: new BearerTokenAuth({ token: 'token' }),
        requireStructuredErrors: true,
      })

      const params = { warehouse: 'wh', namespace: 'ns', table: 'tbl', purgeRequested: true }

      for (const response of [
        reply,
        { status: 500, body: missingNamespace, contentType: 'application/json' },
      ]) {
        reply = response
        await expect(client[operation](params)).rejects.toMatchObject({
          code: ErrorCode.InternalError,
        })
      }

      reply = { status: 404, body: missingNamespace, contentType: 'application/json' }
      await expect(client[operation](params)).rejects.toMatchObject({
        code: 404,
        type: IcebergErrorType.NoSuchNamespaceException,
      })
    }
  )

  it('applies client-wide structured-error confirmation to authorization failures', async () => {
    const client = new RestCatalogClient({
      catalogUrl: 'http://127.0.0.1/v1',
      auth: {
        authorize() {
          throw new IcebergError('gone', IcebergErrorType.NoSuchNamespaceException, 404)
        },
      },
      requireStructuredErrors: true,
    })

    await expect(client.listTables({ namespace: 'ns', warehouse: 'wh' })).rejects.toMatchObject({
      code: ErrorCode.InternalError,
      message: 'Failed to authorize Iceberg catalog request',
    })
  })

  describe.each([
    ['tableExists', IcebergErrorType.NoSuchTableException],
    ['namespaceExists', IcebergErrorType.NoSuchNamespaceException],
  ] as const)('%s with strict errors', (operation, missingType) => {
    const params = { warehouse: 'wh /', namespace: 'ns /', table: 'tbl /' }

    it('confirms a missing resource with a separately authorized GET', async () => {
      const requests: string[] = []
      const server = await startServer((req, res) => {
        requests.push(`${req.method} ${req.url} ${req.headers['x-authorized-method']}`)
        if (req.method === 'HEAD') {
          res.writeHead(404).end()
        } else {
          res.writeHead(404, { 'Content-Type': 'application/json' })
          res.end(JSON.stringify({ error: { message: 'gone', type: missingType, code: 404 } }))
        }
      })
      const client = new RestCatalogClient({
        catalogUrl: `${server.url}/v1`,
        auth: {
          authorize(req) {
            req.headers.set('x-authorized-method', req.method)
            return req
          },
        },
        requireStructuredErrors: true,
      })

      await expect(client[operation](params)).rejects.toMatchObject({
        code: 404,
        type: missingType,
      })

      const path = `/v1/wh%20%2F/namespaces/ns%20%2F${operation === 'tableExists' ? '/tables/tbl%20%2F' : ''}`
      expect(requests).toEqual([`HEAD ${path} HEAD`, `GET ${path} GET`])
    })

    it('resolves when GET finds a resource after HEAD 404', async () => {
      const methods: string[] = []
      const server = await startServer((req, res) => {
        methods.push(req.method!)
        if (req.method === 'HEAD') res.writeHead(404).end()
        else sendJson(res, { metadata: {}, namespace: ['ns /'] })
      })
      const client = new RestCatalogClient({
        catalogUrl: `${server.url}/v1`,
        auth: new BearerTokenAuth({ token: 'token' }),
        requireStructuredErrors: true,
      })

      await expect(client[operation](params)).resolves.toBeUndefined()
      expect(methods).toEqual(['HEAD', 'GET'])
    })

    it('propagates an unconfirmed GET 404 after HEAD 404', async () => {
      const methods: string[] = []
      const server = await startServer((req, res) => {
        methods.push(req.method!)
        if (req.method === 'HEAD') res.writeHead(404).end()
        else res.writeHead(404, { 'Content-Type': 'text/plain' }).end('not found')
      })
      const client = new RestCatalogClient({
        catalogUrl: `${server.url}/v1`,
        auth: new BearerTokenAuth({ token: 'token' }),
        requireStructuredErrors: true,
      })

      await expect(client[operation](params)).rejects.toMatchObject({
        code: ErrorCode.InternalError,
      })
      expect(methods).toEqual(['HEAD', 'GET'])
    })
  })

  it('preserves large integers when sending and parsing JSON bodies', async () => {
    const largeFieldId = JSONBigint.parse('9223372036854775807') as number
    let requestBody = ''
    const server = await startServer(async (req, res) => {
      requestBody = await readRequestBody(req)
      sendJson(res, {
        'metadata-location': 's3://warehouse/ns/table/metadata.json',
        metadata: {
          'current-snapshot-id': JSONBigint.parse('9223372036854775807'),
          'format-version': 2,
          'table-uuid': 'table-id',
        },
      })
    })

    const client = new RestCatalogClient({
      auth: new BearerTokenAuth({ token: 'token' }),
      catalogUrl: `${server.url}/v1`,
    })

    const response = await client.createTable({
      name: 'tbl',
      namespace: 'ns',
      schema: {
        fields: [
          {
            id: largeFieldId,
            name: 'id',
            required: true,
            type: 'long',
          },
        ],
        type: 'struct',
      },
      spec: { fields: [] },
      warehouse: 'wh',
    })

    expect(requestBody).toContain('"id":9223372036854775807')
    expect(String(response.metadata['current-snapshot-id'])).toBe('9223372036854775807')
  })

  it('reports timed out requests as timed out rather than aborted', async () => {
    const server = await startServer(() => {
      // Intentionally leave the response open until the client-side timeout aborts it.
    })
    const client = new RestCatalogClient({
      auth: new BearerTokenAuth({ token: 'token' }),
      catalogUrl: `${server.url}/v1`,
      timeoutMs: 100,
    })

    await expect(client.getConfig({ warehouse: 'wh' })).rejects.toMatchObject({
      code: ErrorCode.InternalError,
      message: 'Iceberg catalog request timed out',
    } satisfies Partial<StorageBackendError>)
  })

  it('rejects successful non-JSON responses', async () => {
    const server = await startServer((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/plain' })
      res.end('ok')
    })
    const client = new RestCatalogClient({
      auth: new BearerTokenAuth({ token: 'token' }),
      catalogUrl: `${server.url}/v1`,
    })

    await expect(client.getConfig({ warehouse: 'wh' })).rejects.toMatchObject({
      code: ErrorCode.InternalError,
      message: 'Unexpected non-JSON response from Iceberg catalog',
      originalError: expect.objectContaining({
        message: 'Unexpected Content-Type: text/plain',
      }),
    } satisfies Partial<StorageBackendError>)
  })

  it('sends one real SigV4 authorization header through fetch', async () => {
    const requests: RequestRecord[] = []
    const server = await startServer(async (req, res) => {
      requests.push({
        body: await readRequestBody(req),
        headers: req.headers,
        method: req.method,
        rawHeaders: req.rawHeaders,
        url: req.url,
      })
      sendJson(res, { defaults: {} })
    })
    const client = new RestCatalogClient({
      auth: new SignV4Auth({
        credentials: {
          accessKeyId: 'AKIDEXAMPLE',
          secretAccessKey: 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY',
        },
        region: 'us-east-1',
      }),
      catalogUrl: `${server.url}/v1`,
    })

    await client.getConfig({ warehouse: 'wh' })

    expect(requests).toHaveLength(1)
    const request = requests[0]
    expect(request.headers.authorization).toContain('AWS4-HMAC-SHA256')
    expect(request.headers['x-amz-date']).toMatch(/^\d{8}T\d{6}Z$/)
    expect(countRawHeader(request.rawHeaders, 'authorization')).toBe(1)
    expect(countRawHeader(request.rawHeaders, 'x-amz-date')).toBe(1)
    expect(countRawHeader(request.rawHeaders, 'host')).toBe(1)
  })
})

async function startServer(
  handler: (req: IncomingMessage, res: ServerResponse) => void | Promise<void>
): Promise<TestServer> {
  const sockets = new Set<Socket>()
  const server = createServer((req, res) => {
    void Promise.resolve(handler(req, res)).catch((error) => {
      res.writeHead(500, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ error: String(error) }))
    })
  })

  server.on('connection', (socket) => {
    sockets.add(socket)
    socket.once('close', () => sockets.delete(socket))
  })

  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve)
  })

  const address = server.address() as AddressInfo
  const testServer = {
    close: async () => {
      for (const socket of sockets) {
        socket.destroy()
      }
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error)
            return
          }
          resolve()
        })
      })
    },
    url: `http://127.0.0.1:${address.port}`,
  }
  servers.push(testServer)
  return testServer
}

async function readRequestBody(req: IncomingMessage) {
  const chunks: Buffer[] = []
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
  }
  return Buffer.concat(chunks).toString('utf8')
}

function sendJson(res: ServerResponse, body: unknown) {
  res.writeHead(200, { 'Content-Type': 'application/json' })
  res.end(JSONBigint.stringify(body))
}

function countRawHeader(rawHeaders: string[], header: string) {
  let count = 0
  for (let index = 0; index < rawHeaders.length; index += 2) {
    if (rawHeaders[index].toLowerCase() === header) {
      count++
    }
  }
  return count
}
