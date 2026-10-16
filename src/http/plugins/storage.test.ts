import type { StorageBackendAdapter } from '@storage/backend'
import Fastify, { type FastifyInstance } from 'fastify'
import { vi } from 'vitest'

const { createStorageBackend } = vi.hoisted(() => ({ createStorageBackend: vi.fn() }))

vi.mock('@storage/backend', () => ({ createStorageBackend }))
vi.mock('@storage/database', () => ({ StoragePgDB: class {} }))
vi.mock('@storage/storage', () => ({ Storage: class {} }))
vi.mock('@storage/locator', () => ({ PassThroughLocation: class {}, TenantLocation: class {} }))
vi.mock('../../config', () => ({
  getConfig: () => ({ storageBackendType: 's3', storageS3Bucket: 'probe' }),
}))

describe('shared HTTP storage backend', () => {
  let apps: FastifyInstance[]
  let observed: StorageBackendAdapter[]

  beforeEach(() => {
    vi.resetModules()
    createStorageBackend.mockReset()
    apps = []
    observed = []
  })

  afterEach(async () => {
    await Promise.all(apps.map((app) => app.close()))
  })

  async function build() {
    const { storage } = await import('./storage')
    const app = Fastify()
    apps.push(app)
    app.register(storage)
    app.get('/', async (req) => {
      observed.push(req.backend)
      return { ok: true }
    })
    return app
  }

  it('keeps other apps usable and recreates the backend during owner shutdown', async () => {
    const retired = Promise.withResolvers<void>()
    const first = { close: vi.fn(() => retired.promise) }
    const next = { close: vi.fn() }
    createStorageBackend.mockReturnValueOnce(first).mockReturnValue(next)
    const { closeStorageBackend } = await import('./storage')
    const firstApp = await build()
    const secondApp = await build()

    expect((await firstApp.inject('/')).statusCode).toBe(200)
    await firstApp.close()
    expect((await secondApp.inject('/')).statusCode).toBe(200)
    expect(observed).toEqual([first, first])
    expect(first.close).not.toHaveBeenCalled()
    await secondApp.close()

    const closed = vi.fn()
    const closing = closeStorageBackend().then(closed)
    await closeStorageBackend()
    const replacement = await build()
    expect((await replacement.inject('/')).statusCode).toBe(200)
    expect(observed.at(-1)).toBe(next)
    expect(first.close).toHaveBeenCalledOnce()
    expect(closed).not.toHaveBeenCalled()

    retired.resolve()
    await closing
    expect((await replacement.inject('/')).statusCode).toBe(200)
    expect(observed.at(-1)).toBe(next)
    expect(createStorageBackend).toHaveBeenCalledTimes(2)
    await replacement.close()
    await closeStorageBackend()
    expect(next.close).toHaveBeenCalledOnce()
  })

  it('propagates a close failure and creates a usable replacement backend', async () => {
    const closeError = new Error('backend close failed')
    const first = { close: vi.fn().mockRejectedValue(closeError) }
    const next = { close: vi.fn() }
    createStorageBackend.mockReturnValueOnce(first).mockReturnValue(next)
    const { closeStorageBackend } = await import('./storage')
    const firstApp = await build()
    expect((await firstApp.inject('/')).statusCode).toBe(200)
    await firstApp.close()

    await expect(closeStorageBackend()).rejects.toBe(closeError)
    const replacement = await build()
    expect((await replacement.inject('/')).statusCode).toBe(200)
    expect(observed.at(-1)).toBe(next)
    await replacement.close()
    await closeStorageBackend()
  })
})
