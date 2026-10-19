import { once } from 'node:events'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import { AcceptanceHttpClient } from './http'
import { cleanupIcebergResources } from './iceberg'

describe('Iceberg acceptance cleanup', () => {
  let server: Server
  let client: AcceptanceHttpClient
  let respond: (request: IncomingMessage, response: ServerResponse) => void
  let requests: string[]
  const namespace = '/iceberg/v1/bucket/namespaces/namespace'

  beforeEach(async () => {
    vi.stubEnv('ACCEPTANCE_TARGET', 'local')
    vi.spyOn(console, 'log').mockImplementation(() => undefined)
    requests = []
    server = createServer((request, response) => {
      requests.push(`${request.method} ${request.url}`)
      respond(request, response)
    })
    server.listen(0, '127.0.0.1')
    await once(server, 'listening')
    client = new AcceptanceHttpClient(`http://127.0.0.1:${(server.address() as AddressInfo).port}`)
  })

  afterEach(async () => {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()))
      server.closeAllConnections()
    })
  })

  const cleanup = () =>
    cleanupIcebergResources({
      client,
      token: 'test-token',
      bucketName: 'bucket',
      namespaceName: 'namespace',
      tableNames: ['first', 'second'],
    })

  it('purges both tables before deleting the namespace and bucket', async () => {
    respond = (request, response) => {
      response.writeHead(request.url === '/iceberg/bucket/bucket' ? 200 : 204).end()
    }

    await cleanup()

    expect(requests).toEqual([
      `DELETE ${namespace}/tables/second?purgeRequested=true`,
      `DELETE ${namespace}/tables/first?purgeRequested=true`,
      `DELETE ${namespace}`,
      'DELETE /iceberg/bucket/bucket',
    ])
  })

  it('reports cleanup failures after attempting every remaining resource', async () => {
    const statuses: Record<string, number> = { first: 204, second: 400, namespace: 500 }
    respond = (request, response) => {
      const name = new URL(request.url!, 'http://localhost').pathname.split('/').at(-1)!
      response.writeHead(statuses[name] ?? 200).end('cleanup response')
    }

    const error = await cleanup().catch((error: unknown) => error)

    expect(error).toBeInstanceOf(AggregateError)
    expect((error as AggregateError).errors).toHaveLength(2)
    expect((error as AggregateError).errors[0].message).toContain('received: 400')
    expect((error as AggregateError).errors[1].message).toContain('received: 500')
    expect(requests).toHaveLength(4)
    expect(requests.at(-1)).toBe('DELETE /iceberg/bucket/bucket')
  })

  it('allows resources that are already absent', async () => {
    respond = (_request, response) => response.writeHead(404).end()

    await expect(cleanup()).resolves.toBeUndefined()
    expect(requests).toHaveLength(4)
  })

  it('still attempts bucket cleanup if namespace creation failed', async () => {
    respond = (_request, response) => response.writeHead(200).end()

    await cleanupIcebergResources({
      client,
      token: 'test-token',
      bucketName: 'bucket',
      tableNames: [],
    })

    expect(requests).toEqual(['DELETE /iceberg/bucket/bucket'])
  })
})
