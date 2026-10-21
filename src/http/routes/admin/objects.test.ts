import { type IncomingMessage, request } from 'node:http'
import { text } from 'node:stream/consumers'
import Fastify from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import { getConfig, mergeConfig } from '../../../config'
import { signals } from '../../plugins/signals'

const mocks = vi.hoisted(() => ({
  dispose: vi.fn(),
  scan: vi.fn<
    (
      bucket: string,
      options: { signal: AbortSignal }
    ) => AsyncGenerator<{ value: { name: string; size: number }[] }, void, unknown>
  >(),
}))

vi.mock('@internal/database', () => ({
  getPostgresConnection: async () => ({ dispose: mocks.dispose }),
  getServiceKeyUser: async () => ({ jwt: 'service-jwt', payload: { role: 'service_role' } }),
}))
vi.mock('@internal/database/migrations', () => ({
  lastLocalMigrationName: async () => 'initialmigration',
}))
vi.mock('@storage/database', () => ({ S3_KEYS_SCRATCH_TABLE_QUALIFIED_PATTERN: '[a-z]+' }))
vi.mock('@storage/scanner/scanner', () => ({
  ObjectScanner: class {
    listOrphaned = mocks.scan
    deleteOrphans = mocks.scan
  },
}))
vi.mock('../../plugins', async () => ({
  dbSuperUser: (await import('../../plugins/db')).dbSuperUser,
  storage: fastifyPlugin(async () => {}),
}))

let routes: typeof import('./objects').default

beforeAll(async () => {
  getConfig({ reload: true })
  mergeConfig({
    adminApiKeys: 'test-admin-key',
    databaseEnableQueryCancellation: false,
    isMultitenant: false,
  })
  routes = (await import('./objects')).default
})

describe.each(['GET', 'DELETE'] as const)('%s orphan-object stream cleanup', (method) => {
  it.each(['success', 'error', 'disconnect'] as const)(
    'releases the lease after scanner cleanup on %s',
    async (outcome) => {
      const continueScan = Promise.withResolvers<void>()
      const cleanupStarted = Promise.withResolvers<void>()
      const finishCleanup = Promise.withResolvers<void>()
      let scanSignal: AbortSignal | undefined
      mocks.dispose.mockClear()
      mocks.scan.mockImplementation(async function* (_bucket, { signal }) {
        scanSignal = signal
        try {
          yield { value: [{ name: 'orphan', size: 1 }] }
          await continueScan.promise
          if (outcome === 'error') throw new Error('scanner failed')
        } finally {
          cleanupStarted.resolve()
          await finishCleanup.promise
        }
      })

      const app = Fastify()
      app.decorateRequest('tenantId')
      app.addHook('onRequest', async (req) => {
        req.tenantId = 'tenant-id'
      })
      await app.register(signals)
      await app.register(routes)
      const baseUrl = await app.listen({ host: '127.0.0.1', port: 0 })
      const incoming = Promise.withResolvers<IncomingMessage>()
      const body = JSON.stringify({ deleteDbKeys: true })
      const req = request(
        `${baseUrl}/tenant-id/buckets/bucket-id/orphan-objects`,
        {
          method,
          headers: {
            apikey: 'test-admin-key',
            ...(method === 'DELETE'
              ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) }
              : {}),
          },
        },
        incoming.resolve
      )
      req.on('error', incoming.reject)
      req.end(method === 'DELETE' ? body : undefined)
      let res: IncomingMessage | undefined

      try {
        res = await incoming.promise
        expect(res.statusCode).toBe(200)
        let responseBody: Promise<string> | undefined
        if (outcome === 'disconnect') {
          res.on('error', () => {})
          res.destroy()
          await vi.waitFor(() => expect(scanSignal?.aborted).toBe(true))
        } else {
          responseBody = text(res)
        }

        continueScan.resolve()
        await cleanupStarted.promise
        expect(mocks.dispose).not.toHaveBeenCalled()
        finishCleanup.resolve()
        await vi.waitFor(() => expect(mocks.dispose).toHaveBeenCalledOnce())

        if (responseBody) {
          const received = await responseBody
          expect(received).toContain('"event":"data"')
          expect(received.includes('"event":"error"')).toBe(outcome === 'error')
        }
      } finally {
        continueScan.resolve()
        finishCleanup.resolve()
        req.destroy()
        res?.destroy()
        app.server.closeAllConnections()
        await app.close()
      }
    }
  )
})
