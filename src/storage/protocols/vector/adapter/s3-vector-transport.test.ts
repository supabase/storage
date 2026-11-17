import http from 'node:http'
import type { AddressInfo } from 'node:net'
import { PutVectorsCommand, QueryVectorsCommand } from '@aws-sdk/client-s3vectors'
import { UndiciHttpHandler } from '@smithy/undici-http-handler'
import { vi } from 'vitest'
import { createS3VectorClient } from './s3-vector'

const query = {
  vectorBucketName: 'physical-vectors-a',
  indexName: 'test-index',
  queryVector: { float32: [1, 2, 3] },
  topK: 1,
}
const result = { distanceMetric: 'cosine', vectors: [{ key: 'matched-key', distance: 0.1 }] }

describe('S3 vector HTTP transport', () => {
  const clients: ReturnType<typeof createS3VectorClient>[] = []
  const servers: http.Server[] = []

  async function server(handler: http.RequestListener) {
    const instance = http.createServer(handler)
    servers.push(instance)
    await new Promise<void>((resolve) => instance.listen(0, '127.0.0.1', resolve))
    return `http://127.0.0.1:${(instance.address() as AddressInfo).port}`
  }

  function client(endpoint: string) {
    vi.stubEnv('AWS_ENDPOINT_URL', endpoint)
    vi.stubEnv('AWS_ACCESS_KEY_ID', 'local-test')
    vi.stubEnv('AWS_SECRET_ACCESS_KEY', 'local-test')
    vi.stubEnv('AWS_EC2_METADATA_DISABLED', 'true')
    const instance = createS3VectorClient()
    instance.config.maxAttempts = async () => 1
    clients.push(instance)
    return instance
  }

  afterEach(async () => {
    for (const instance of clients.splice(0)) {
      await instance.close()
    }
    await Promise.all(
      servers.splice(0).map(
        (instance) =>
          new Promise<void>((resolve, reject) => {
            instance.closeAllConnections()
            instance.close((error) => (error ? reject(error) : resolve()))
          })
      )
    )
  })

  it('sends signed queries and batch writes for different buckets to the configured endpoint', async () => {
    const requests: { path?: string; authorization?: string; body: unknown }[] = []
    const endpoint = await server(async (req, res) => {
      const chunks: Buffer[] = []
      for await (const chunk of req) chunks.push(chunk)
      requests.push({
        path: req.url,
        authorization: req.headers.authorization,
        body: JSON.parse(Buffer.concat(chunks).toString()),
      })
      res.setHeader('content-type', 'application/json')
      res.end(req.url?.endsWith('/QueryVectors') ? JSON.stringify(result) : '')
    })
    const sdk = client(`${endpoint}/custom`)
    expect(sdk.config.requestHandler).toBeInstanceOf(UndiciHttpHandler)
    await expect(sdk.send(new QueryVectorsCommand(query))).resolves.toMatchObject(result)
    const batch = {
      vectorBucketName: 'physical-vectors-b',
      indexName: 'test-index',
      vectors: Array.from({ length: 100 }, (_, i) => ({
        key: `vector-${i}`,
        data: { float32: Array.from({ length: 768 }, (_, d) => d / 1000) },
      })),
    }
    await sdk.send(new PutVectorsCommand(batch))

    expect(requests).toEqual([
      {
        path: '/custom/QueryVectors',
        authorization: expect.stringMatching(/^AWS4-HMAC-SHA256 /),
        body: query,
      },
      {
        path: '/custom/PutVectors',
        authorization: expect.stringMatching(/^AWS4-HMAC-SHA256 /),
        body: batch,
      },
    ])
  })

  it('preserves AWS error decoding and retries transient service failures', async () => {
    let requests = 0
    const endpoint = await server((req, res) => {
      req.resume()
      res.setHeader('content-type', 'application/json')
      res.setHeader('x-amzn-requestid', 'vector-request-id')
      if (++requests === 1) {
        res.writeHead(503, { 'x-amzn-errortype': 'ServiceUnavailableException' })
        res.end(JSON.stringify({ message: 'Retry this request' }))
      } else if (requests === 2) {
        res.end(JSON.stringify(result))
      } else {
        res.writeHead(404, { 'x-amzn-errortype': 'NotFoundException' })
        res.end(JSON.stringify({ message: 'Index does not exist' }))
      }
    })
    const sdk = client(endpoint)
    sdk.config.maxAttempts = async () => 2
    await expect(sdk.send(new QueryVectorsCommand(query))).resolves.toMatchObject({
      ...result,
      $metadata: { attempts: 2 },
    })
    await expect(sdk.send(new QueryVectorsCommand(query))).rejects.toMatchObject({
      name: 'NotFoundException',
      message: 'Index does not exist',
      $metadata: { httpStatusCode: 404, requestId: 'vector-request-id', attempts: 1 },
    })
  })

  it('waits for a connection when all sockets are occupied', async () => {
    const held = Promise.withResolvers<void>()
    let started = 0
    const endpoint = await server(async (req, res) => {
      req.resume()
      started++
      if (started <= 50) await held.promise
      res.setHeader('content-type', 'application/json')
      res.end(JSON.stringify(result))
    })
    const sdk = client(endpoint)
    const requests = Array.from({ length: 51 }, () => sdk.send(new QueryVectorsCommand(query)))
    const settledRequests = Promise.allSettled(requests)
    try {
      await vi.waitFor(() => expect(started).toBe(50))
      // Outlast the shared agent's 5 s socket deadline; a request that hit it would retry.
      await new Promise((resolve) => setTimeout(resolve, 5200))
      held.resolve()
      const results = await Promise.all(requests)
      expect(results.map((result) => result.$metadata.attempts)).toEqual(Array(51).fill(1))
    } finally {
      held.resolve()
      await settledRequests
    }
  }, 10000)

  it('retries a connection lost while reading a JSON response body', async () => {
    let requests = 0
    const endpoint = await server((req, res) => {
      req.resume()
      res.setHeader('content-type', 'application/json')
      if (++requests === 1) {
        res.setHeader('content-length', Buffer.byteLength(JSON.stringify(result)))
        res.write('{"vectors":[')
        setTimeout(() => res.destroy(), 20)
      } else {
        res.end(JSON.stringify(result))
      }
    })
    const sdk = client(endpoint)
    sdk.config.maxAttempts = async () => 2
    await expect(sdk.send(new QueryVectorsCommand(query))).resolves.toMatchObject({
      ...result,
      $metadata: { attempts: 2 },
    })
    expect(requests).toBe(2)
  })
})
