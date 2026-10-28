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

describe('HTTP storage backend ownership', () => {
  let apps: FastifyInstance[]
  let observed: StorageBackendAdapter[]

  beforeEach(() => {
    createStorageBackend.mockReset()
    apps = []
    observed = []
  })

  afterEach(async () => {
    await Promise.all(apps.map((app) => app.close()))
  })

  async function build(owner: import('./storage').SharedStorageBackend) {
    const { storage } = await import('./storage')
    const app = Fastify()
    apps.push(app)
    app.decorate('sharedStorageBackend', owner)
    app.register(storage)
    app.get('/', async (req) => {
      observed.push(req.backend)
      return { ok: true }
    })
    return app
  }

  it('shares one owner across apps and keeps independent owners isolated', async () => {
    const { SharedStorageBackend } = await import('./storage')
    const first = { close: vi.fn() }
    const second = { close: vi.fn() }
    createStorageBackend.mockReturnValueOnce(first).mockReturnValueOnce(second)
    const shared = new SharedStorageBackend()
    const independent = new SharedStorageBackend()
    const firstApp = await build(shared)
    const secondApp = await build(shared)
    const thirdApp = await build(independent)
    expect((await firstApp.inject('/')).statusCode).toBe(200)
    await firstApp.close()
    expect((await secondApp.inject('/')).statusCode).toBe(200)
    expect((await thirdApp.inject('/')).statusCode).toBe(200)
    expect(observed).toEqual([first, first, second])
    expect(createStorageBackend).toHaveBeenCalledTimes(2)
    expect(first.close).not.toHaveBeenCalled()
    await shared.close()
    expect(first.close).toHaveBeenCalledOnce()
    expect(second.close).not.toHaveBeenCalled()
    await independent.close()
    expect(second.close).toHaveBeenCalledOnce()
  })

  it('shares an app owner through sibling route scopes', async () => {
    const { storage, SharedStorageBackend } = await import('./storage')
    const backend = { close: vi.fn() }
    createStorageBackend.mockReturnValue(backend)
    const owner = new SharedStorageBackend()
    const app = Fastify()
    apps.push(app)
    app.decorate('sharedStorageBackend', owner)
    for (const prefix of ['/first', '/second']) {
      app.register(
        async (scope) => {
          scope.register(storage)
          scope.get('/', async (req) => {
            observed.push(req.backend)
            return { ok: true }
          })
        },
        { prefix }
      )
    }
    expect((await app.inject('/first/')).statusCode).toBe(200)
    expect((await app.inject('/second/')).statusCode).toBe(200)
    expect(observed).toEqual([backend, backend])
    expect(createStorageBackend).toHaveBeenCalledOnce()
    await app.close()
    expect(backend.close).not.toHaveBeenCalled()
    await owner.close()
    expect(backend.close).toHaveBeenCalledOnce()
  })

  it('replaces a retired backend while its close is pending', async () => {
    const { SharedStorageBackend } = await import('./storage')
    const retired = Promise.withResolvers<void>()
    const first = { close: vi.fn(() => retired.promise) }
    const next = { close: vi.fn() }
    createStorageBackend.mockReturnValueOnce(first).mockReturnValue(next)
    const owner = new SharedStorageBackend()
    const app = await build(owner)
    await app.inject('/')
    await app.close()
    const closing = owner.close()
    await owner.close()
    const replacement = await build(owner)
    await replacement.inject('/')
    expect(observed.at(-1)).toBe(next)
    expect(first.close).toHaveBeenCalledOnce()
    retired.resolve()
    await closing
    await owner.close()
    expect(next.close).toHaveBeenCalledOnce()
  })

  it('propagates close failures and allows a replacement', async () => {
    const { SharedStorageBackend } = await import('./storage')
    const error = new Error('backend close failed')
    const first = { close: vi.fn().mockRejectedValue(error) }
    const next = { close: vi.fn() }
    createStorageBackend.mockReturnValueOnce(first).mockReturnValueOnce(next)
    const owner = new SharedStorageBackend()
    const app = await build(owner)
    await app.inject('/')
    await app.close()
    await expect(owner.close()).rejects.toBe(error)
    const replacement = await build(owner)
    await replacement.inject('/')
    expect(observed.at(-1)).toBe(next)
    await replacement.close()
    await owner.close()
    expect(next.close).toHaveBeenCalledOnce()
  })
})
