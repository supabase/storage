import { createServer, type Server, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import { normalizeRawError } from '@internal/errors'
import * as k8s from '@kubernetes/client-node'
import { vi } from 'vitest'
import { Cluster } from './cluster'
import { ClusterDiscoveryEKS } from './eks'

const config = vi.hoisted(() => ({
  clusterDiscoveryTimeoutMs: 1500,
  clusterDiscoveryPollIntervalMs: 50,
  clusterDiscoveryEcsMaxRps: 10,
  numWorkers: 1,
}))
vi.mock('../../config', () => ({ getConfig: () => config }))
vi.mock('@internal/monitoring', () => ({ logger: { info: vi.fn() } }))

const podList = {
  apiVersion: 'v1',
  kind: 'PodList',
  metadata: {},
  items: ['Pending', 'Running', 'Succeeded', 'Failed', 'Unknown', undefined].map((phase) => ({
    status: { phase },
  })),
}

describe('EKS discovery HTTP cancellation', () => {
  let server: Server
  let shutdown: AbortController
  let stalledResponse: ServerResponse | undefined
  let calls: number
  let stall: 'headers' | 'body' | undefined
  let status: number
  let recoveredAfterClose: boolean

  beforeEach(async () => {
    config.clusterDiscoveryTimeoutMs = 1500
    shutdown = new AbortController()
    stalledResponse = undefined
    calls = 0
    stall = undefined
    status = 200
    recoveredAfterClose = false
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    vi.spyOn(console, 'error').mockImplementation(() => {})
    server = createServer((request, response) => {
      request.resume()
      calls++
      expect(request.headers.authorization).toBe('Bearer local-service-account-token')
      expect(request.headers['user-agent']).toMatch(/^kubernetes-client-javascript\//)
      const url = new URL(request.url!, 'http://localhost')
      expect(url.pathname).toBe('/api/v1/namespaces/storage-test/pods')
      expect(url.searchParams.get('labelSelector')).toBe('app=storage,tier in (api,worker)')
      response.setHeader('Content-Type', 'application/json')
      if (stall && !stalledResponse) {
        stalledResponse = response
        if (stall === 'body') {
          response.writeHead(status)
          response.write('{')
        }
        return
      }
      if (stalledResponse) recoveredAfterClose = stalledResponse.destroyed
      response.writeHead(status)
      if (status !== 200) {
        response.end(
          JSON.stringify({
            apiVersion: 'v1',
            kind: 'Status',
            status: 'Failure',
            reason: 'Forbidden',
            message: 'local denial',
            code: status,
          })
        )
        return
      }
      response.end(
        JSON.stringify({
          ...podList,
          items: podList.items.concat(stalledResponse ? [{ status: { phase: 'Running' } }] : []),
        })
      )
    })
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject)
      server.listen(0, '127.0.0.1', resolve)
    })
    const endpoint = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
    vi.spyOn(k8s.KubeConfig.prototype, 'loadFromCluster').mockImplementation(function (
      this: k8s.KubeConfig
    ) {
      this.loadFromClusterAndUser(
        { name: 'local', server: endpoint, skipTLSVerify: true },
        { name: 'local', token: 'local-service-account-token' }
      )
    })
    vi.stubEnv('CLUSTER_DISCOVERY', 'EKS')
    vi.stubEnv('KUBERNETES_SERVICE_HOST', '127.0.0.1')
    vi.stubEnv('KUBERNETES_NAMESPACE', 'storage-test')
    vi.stubEnv('KUBERNETES_LABEL_SELECTOR', 'app=storage,tier in (api,worker)')
  })

  afterEach(async () => {
    shutdown.abort()
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  })

  it('preserves bearer authentication and pod filtering', async () => {
    await expect(new ClusterDiscoveryEKS().getClusterSize(shutdown.signal)).resolves.toBe(2)
    expect(calls).toBe(1)
  })

  it('keeps API errors instead of reporting an empty cluster', async () => {
    status = 403
    await expect(new ClusterDiscoveryEKS().getClusterSize(shutdown.signal)).rejects.toThrow('403')
  })

  it('preserves the transport error through the discovery wrapper', async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()))

    const error = await new ClusterDiscoveryEKS()
      .getClusterSize(shutdown.signal)
      .catch((error: unknown) => error)
    expect(error).toMatchObject({
      cause: {
        message: 'fetch failed',
        cause: { code: 'ECONNREFUSED' },
      },
    })
    expect(normalizeRawError(error, 'info').raw).toContain('"code":"ECONNREFUSED"')
  })

  it.each([
    'headers',
    'body',
  ] as const)('closes a stalled %s response at the discovery deadline', async (part) => {
    stall = part
    config.clusterDiscoveryTimeoutMs = 200
    await expect(Cluster.init(shutdown.signal)).rejects.toMatchObject({ name: 'TimeoutError' })
    await vi.waitFor(() => expect(stalledResponse?.destroyed).toBe(true))
    expect(calls).toBe(1)
  })

  it('closes a stalled poll before the next poll and resumes discovery', async () => {
    config.clusterDiscoveryTimeoutMs = 200
    await Cluster.init(shutdown.signal)
    stall = 'body'
    await vi.waitFor(() => expect(stalledResponse).toBeDefined())
    await vi.waitFor(() => expect(Cluster.size).toBe(3))
    expect(recoveredAfterClose).toBe(true)
    expect(calls).toBeGreaterThanOrEqual(3)
  })

  it('does not send a request with an already aborted signal', async () => {
    shutdown.abort()
    await expect(new ClusterDiscoveryEKS().getClusterSize(shutdown.signal)).rejects.toThrow()
    expect(calls).toBe(0)
  })

  it('keeps cancellation separate for concurrent discovery calls', async () => {
    stall = 'body'
    const discovery = new ClusterDiscoveryEKS()
    const first = discovery.getClusterSize(shutdown.signal).catch((error: unknown) => error)
    await vi.waitFor(() => expect(stalledResponse).toBeDefined())
    await expect(discovery.getClusterSize(new AbortController().signal)).resolves.toBe(3)
    expect(stalledResponse?.destroyed).toBe(false)

    shutdown.abort()
    expect(await first).toBeInstanceOf(Error)
    await vi.waitFor(() => expect(stalledResponse?.destroyed).toBe(true))
    expect(calls).toBe(2)
  })
})
