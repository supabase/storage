import { createServer } from 'node:http'
import { createRequire } from 'node:module'
import type { AddressInfo } from 'node:net'

const state = vi.hoisted(() => ({ url: '' }))

vi.mock('../../../config', () => ({
  getConfig: () => ({
    webhookURL: state.url,
    webhookMaxConnections: 2,
    webhookQueueMaxFreeSockets: 1,
  }),
}))
vi.mock('@internal/database', () => ({ getTenantConfig: vi.fn() }))
vi.mock('@internal/monitoring', () => ({
  logger: { debug: vi.fn(), error: vi.fn(), info: vi.fn() },
  logSchema: { event: vi.fn() },
}))
// Minimal stand-ins: enough class surface for QueueHandler, without pulling base.ts's
// storage/database import graph (or the full queue registry) into the unit test.
vi.mock('../base', () => ({
  storageEvent: (opts: { type: string }) =>
    class {
      static readonly eventType = opts.type
      constructor(readonly data: unknown) {}
    },
}))
vi.mock('../queue', () => ({ getStorageQueue: vi.fn() }))
vi.mock('../registry', () => ({
  QUEUES: { webhooks: 'webhooks' },
  defaultRetry: (name: string) => ({
    maxAttempts: 21,
    backoffMs: 5_000,
    deadLetter: `${name}-dead-letter`,
  }),
}))

it.each(['native', 'watt'])('delivers a webhook with %s fetch', async (runtime) => {
  const requests: string[] = []
  const server = createServer((request, response) => {
    let body = ''
    request.setEncoding('utf8')
    request.on('data', (chunk) => {
      body += chunk
    })
    request.on('end', () => {
      requests.push(body)
      response.writeHead(204).end()
    })
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  state.url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`

  try {
    if (runtime === 'watt') {
      // Watt installs the Undici globals resolved from its own runtime package.
      const runtimeRequire = createRequire(require.resolve('@platformatic/runtime'))
      const wattUndici = runtimeRequire('undici') as typeof import('undici')
      vi.stubGlobal('fetch', wattUndici.fetch)
      vi.stubGlobal('Headers', wattUndici.Headers)
    }
    vi.resetModules()
    const { WebhookHandler } = await import('./webhook')
    const event = {
      $version: '1',
      type: 'ObjectCreated:Put',
      region: 'test',
      applyTime: 0,
      payload: { bucketId: 'bucket', name: 'object' },
    }
    await new WebhookHandler().handle({
      queue: 'webhooks',
      message: {
        id: 'job',
        data: { event, tenant: { ref: 'tenant', host: 'localhost' }, region: 'test' },
        headers: {},
        timestamp: 0,
        attempt: 1,
      },
      signal: new AbortController().signal,
      heartbeat: async () => {},
    })

    expect(requests).toHaveLength(1)
    expect(JSON.parse(requests[0])).toMatchObject({ type: 'Webhook', event })
  } finally {
    vi.unstubAllGlobals()
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  }
})
