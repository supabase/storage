import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { createServer, type Server, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { ECSClient } from '@aws-sdk/client-ecs'
import { vi } from 'vitest'
import { Cluster } from './cluster'

const transport = vi.hoisted(() => ({ endpoint: '', clients: [] as ECSClient[], timeoutMs: 0 }))

vi.mock('@aws-sdk/client-ecs', async () => {
  const sdk = await vi.importActual<typeof import('@aws-sdk/client-ecs')>('@aws-sdk/client-ecs')
  return {
    ...sdk,
    ECSClient: class extends sdk.ECSClient {
      constructor(options: ConstructorParameters<typeof sdk.ECSClient>[0]) {
        super({
          ...options,
          endpoint: transport.endpoint,
          region: 'us-east-1',
          credentials: { accessKeyId: 'LOCALONLY', secretAccessKey: 'local-only' },
        })
        transport.clients.push(this)
      }
    },
  }
})

vi.mock('../../config', () => ({
  getConfig: () => ({
    clusterDiscoveryTimeoutMs: transport.timeoutMs,
    clusterDiscoveryPollIntervalMs: 50,
    clusterDiscoveryEcsMaxRps: 10,
    numWorkers: 1,
  }),
}))
vi.mock('@internal/monitoring', () => ({ logger: { info: vi.fn() } }))

describe('ECS discovery HTTP cancellation', () => {
  let server: Server
  let shutdown: AbortController
  let stalledResponse: ServerResponse | undefined
  let listCalls: number
  let stallMetadata: boolean
  let throttle: boolean
  let paginate: boolean
  let recoveredAfterClose: boolean
  let configDirectory: string

  function stallResponse(response: ServerResponse) {
    stalledResponse = response
    response.writeHead(200)
    response.write('{')
  }

  beforeEach(async () => {
    configDirectory = await mkdtemp(join(tmpdir(), 'storage-ecs-config-'))
    vi.stubEnv('AWS_CONFIG_FILE', join(configDirectory, 'config'))
    vi.stubEnv('AWS_SHARED_CREDENTIALS_FILE', join(configDirectory, 'credentials'))
    vi.stubEnv('AWS_PROFILE', 'storage-test')
    transport.timeoutMs = 1500
    listCalls = 0
    stallMetadata = false
    throttle = false
    paginate = false
    recoveredAfterClose = false
    stalledResponse = undefined
    shutdown = new AbortController()
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    vi.spyOn(console, 'error').mockImplementation(() => {})
    server = createServer((request, response) => {
      request.resume()
      response.setHeader('Content-Type', 'application/x-amz-json-1.1')
      if (request.url === '/task') {
        if (stallMetadata) {
          stallResponse(response)
        } else {
          response.end(JSON.stringify({ Cluster: 'local', Family: 'storage' }))
        }
        return
      }

      listCalls++
      if (throttle) {
        response.writeHead(400)
        response.end(JSON.stringify({ __type: 'ThrottlingException', message: 'local throttle' }))
        return
      }
      if (listCalls === 2) {
        stallResponse(response)
        return
      }
      if (listCalls > 2) recoveredAfterClose = stalledResponse?.destroyed === true
      response.end(
        JSON.stringify({
          taskArns: listCalls === 1 ? ['a', 'b'] : ['a', 'b', 'c'],
          nextToken: paginate && listCalls === 1 ? 'page-2' : undefined,
        })
      )
    })
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject)
      server.listen(0, '127.0.0.1', resolve)
    })
    transport.endpoint = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
    vi.stubEnv('ECS_CONTAINER_METADATA_URI', transport.endpoint)
    vi.stubEnv('CLUSTER_DISCOVERY', 'ECS')
    vi.stubEnv('AWS_MAX_ATTEMPTS', undefined)
    vi.stubEnv('AWS_RETRY_MODE', 'standard')
  })

  afterEach(async () => {
    shutdown.abort()
    for (const client of transport.clients.splice(0)) client.destroy()
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
    await rm(configDirectory, { recursive: true, force: true })
  })

  it('cancels a partial SDK response and resumes polling with the real client', async () => {
    await Cluster.init(shutdown.signal)
    expect(Cluster.size).toBe(2)

    await vi.waitFor(() => expect(Cluster.size).toBe(3), { timeout: 3000 })
    shutdown.abort()

    expect(recoveredAfterClose).toBe(true)
    expect(listCalls).toBeGreaterThanOrEqual(3)
  })

  it('times out a partial metadata response without sending ListTasks', async () => {
    stallMetadata = true
    transport.timeoutMs = 200
    await expect(Cluster.init(shutdown.signal)).rejects.toMatchObject({ name: 'TimeoutError' })

    await vi.waitFor(() => expect(stalledResponse?.destroyed).toBe(true))
    expect(listCalls).toBe(0)
  })

  it('cancels a stalled continuation page without publishing the partial count', async () => {
    paginate = true
    transport.timeoutMs = 200
    const previousSize = Cluster.size
    await expect(Cluster.init(shutdown.signal)).rejects.toMatchObject({ name: 'TimeoutError' })

    await vi.waitFor(() => expect(stalledResponse?.destroyed).toBe(true))
    expect(listCalls).toBe(2)
    expect(Cluster.size).toBe(previousSize)
  })

  it('bounds SDK retry backoff and prevents HTTP retries after the deadline', async () => {
    vi.stubEnv('AWS_MAX_ATTEMPTS', '100')
    throttle = true
    transport.timeoutMs = 200
    await expect(Cluster.init(shutdown.signal)).rejects.toMatchObject({ name: 'TimeoutError' })
    const callsAtDeadline = listCalls
    expect(callsAtDeadline).toBeGreaterThan(0)

    // Wait through the first SDK throttling backoff with random fixed to 0.5.
    await new Promise((resolve) => setTimeout(resolve, 600))
    expect(listCalls).toBe(callsAtDeadline)
  })

  it.each([
    { setting: 'the default', profileAttempts: undefined, attempts: 10 },
    { setting: 'shared profile max_attempts=1', profileAttempts: 1, attempts: 1 },
    { setting: 'shared profile max_attempts=2', profileAttempts: 2, attempts: 2 },
    { setting: 'env over profile', profileAttempts: 1, envAttempts: 2, attempts: 2 },
  ])('honors $setting for retryable failures', async ({
    profileAttempts,
    envAttempts,
    attempts,
  }) => {
    if (envAttempts !== undefined) vi.stubEnv('AWS_MAX_ATTEMPTS', String(envAttempts))
    if (profileAttempts !== undefined) {
      await writeFile(
        join(configDirectory, 'config'),
        `[default]\nmax_attempts=3\n[profile storage-test]\nmax_attempts=${profileAttempts}\n`
      )
    }
    vi.mocked(Math.random).mockReturnValue(0)
    throttle = true

    await expect(Cluster.init(shutdown.signal)).rejects.toMatchObject({
      name: 'ThrottlingException',
      $metadata: { attempts },
    })
    expect(listCalls).toBe(attempts)
  })
})
