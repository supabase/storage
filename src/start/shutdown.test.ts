import type { AsyncAbortController } from '@internal/concurrency'
import Fastify from 'fastify'
import { vi } from 'vitest'

function mockShutdownDeps(opts: { spreadOriginals?: boolean } = {}) {
  const calls: string[] = []
  const original = async (importOriginal: () => Promise<object>) =>
    opts.spreadOriginals ? await importOriginal() : {}
  const stop = vi.fn(async () => {
    calls.push('PgTenantConnection.stop')
  })
  const shutdownMultitenantPg = vi.fn(async () => {
    calls.push('shutdownMultitenantPg')
  })
  const logSchema = { error: vi.fn(), info: vi.fn() }

  vi.doMock('@internal/database', async (importOriginal) => ({
    ...(await original(importOriginal)),
    shutdownMultitenantPg,
    PgTenantConnection: { stop },
  }))
  vi.doMock('@internal/monitoring', async (importOriginal) => ({
    ...(await original(importOriginal)),
    logger: { flush: vi.fn() },
    logSchema,
  }))

  const abortAsync = vi.fn(async () => {
    calls.push('serverSignal.abortAsync')
  })
  const serverSignal = { abortAsync } as unknown as AsyncAbortController
  return { calls, stop, logSchema, abortAsync, serverSignal }
}

async function loadShutdown(opts: { spreadOriginals?: boolean } = {}) {
  const deps = mockShutdownDeps(opts)
  const closeStorageBackend = vi.fn(async () => {
    deps.calls.push('closeStorageBackend')
  })
  const { shutdown } = await import('./shutdown')
  return { shutdown, closeStorageBackend, ...deps }
}

describe('shutdown', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.doUnmock('@internal/database')
    vi.doUnmock('@internal/monitoring')
    vi.doUnmock('@storage/backend')
    vi.doUnmock('@storage/database')
    vi.doUnmock('@storage/storage')
    vi.doUnmock('@storage/locator')
    vi.resetModules()
  })

  it('preserves the shared backend if HTTP shutdown does not settle', async () => {
    vi.useFakeTimers()
    const { shutdown, serverSignal, abortAsync, closeStorageBackend, calls } = await loadShutdown()
    abortAsync.mockImplementation(() => {
      calls.push('serverSignal.abortAsync')
      return new Promise<void>(() => undefined)
    })

    const failure = shutdown(serverSignal, closeStorageBackend).catch((error) => error)
    await vi.advanceTimersByTimeAsync(60_000)

    expect(await failure).toEqual(
      expect.objectContaining({
        message: 'Shutdown phase "abort server signal" timed out after 60000ms',
      })
    )
    expect(closeStorageBackend).not.toHaveBeenCalled()
    expect(calls).toEqual([
      'serverSignal.abortAsync',
      'PgTenantConnection.stop',
      'shutdownMultitenantPg',
    ])
  })

  it('continues database teardown when storage closure rejects', async () => {
    const { shutdown, serverSignal, closeStorageBackend, calls, logSchema } = await loadShutdown()
    const closeError = new Error('storage close failed')
    closeStorageBackend.mockImplementation(async () => {
      calls.push('closeStorageBackend')
      throw closeError
    })

    await expect(shutdown(serverSignal, closeStorageBackend)).rejects.toBe(closeError)
    expect(calls).toEqual([
      'serverSignal.abortAsync',
      'closeStorageBackend',
      'PgTenantConnection.stop',
      'shutdownMultitenantPg',
    ])
    expect(logSchema.error).toHaveBeenCalledWith(
      expect.anything(),
      'Failed to close storage backend',
      expect.objectContaining({ type: 'shutdown', error: closeError })
    )
  })

  it('stops waiting after the storage close grace period and logs a later failure', async () => {
    vi.useFakeTimers()
    const { shutdown, serverSignal, closeStorageBackend, stop, calls, logSchema } =
      await loadShutdown()
    const late = Promise.withResolvers<void>()
    closeStorageBackend.mockImplementation(async () => {
      calls.push('closeStorageBackend')
      await late.promise
    })
    const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout')

    const done = shutdown(serverSignal, closeStorageBackend)
    await vi.advanceTimersByTimeAsync(1_999)
    expect(stop).not.toHaveBeenCalled()
    expect(logSchema.info).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(1)

    await expect(done).resolves.toBeUndefined()
    expect(calls).toEqual([
      'serverSignal.abortAsync',
      'closeStorageBackend',
      'PgTenantConnection.stop',
      'shutdownMultitenantPg',
    ])
    expect(logSchema.info).toHaveBeenCalledWith(
      expect.anything(),
      'Storage backend close still pending after grace period',
      { type: 'shutdown' }
    )
    const grace = setTimeoutSpy.mock.calls.findIndex(([, ms]) => ms === 2_000)
    const timer = setTimeoutSpy.mock.results[grace].value as NodeJS.Timeout
    expect(timer.hasRef()).toBe(false)
    expect(logSchema.error).not.toHaveBeenCalled()

    const closeError = new Error('late storage close failure')
    late.reject(closeError)
    await vi.advanceTimersByTimeAsync(0)
    expect(logSchema.error).toHaveBeenCalledWith(
      expect.anything(),
      'Failed to close storage backend',
      expect.objectContaining({ type: 'shutdown', error: closeError })
    )
  })

  it('rejects when tenant pool drain fails after closing the shared pool', async () => {
    const { shutdown, serverSignal, closeStorageBackend, stop, calls } = await loadShutdown()
    const tenantError = new Error('tenant pool drain failed')
    stop.mockImplementation(async () => {
      calls.push('PgTenantConnection.stop')
      throw tenantError
    })

    await expect(shutdown(serverSignal, closeStorageBackend)).rejects.toBe(tenantError)
    expect(calls).toEqual([
      'serverSignal.abortAsync',
      'closeStorageBackend',
      'PgTenantConnection.stop',
      'shutdownMultitenantPg',
    ])
  })

  it('drains HTTP users and awaits the storage plugin backend before database teardown', async () => {
    const { serverSignal, abortAsync, stop, calls } = mockShutdownDeps()
    const drained = Promise.withResolvers<void>()
    abortAsync.mockImplementation(async () => {
      calls.push('serverSignal.abortAsync')
      await drained.promise
    })
    const retired = Promise.withResolvers<void>()
    const backend = {
      close: vi.fn(() => {
        calls.push('closeStorageBackend')
        return retired.promise
      }),
    }
    vi.doMock('@storage/backend', () => ({ createStorageBackend: () => backend }))
    vi.doMock('@storage/database', () => ({ StoragePgDB: class {} }))
    vi.doMock('@storage/storage', () => ({ Storage: class {} }))
    vi.doMock('@storage/locator', () => ({
      PassThroughLocation: class {},
      TenantLocation: class {},
    }))
    const { storage, SharedStorageBackend } = await import('../http/plugins/storage')
    const owner = new SharedStorageBackend()
    const closeStorageBackend = () => owner.close()
    const { shutdown } = await import('./shutdown')
    const app = Fastify()
    app.decorate('sharedStorageBackend', owner)
    app.register(storage)
    await app.ready()
    await app.close()
    vi.useFakeTimers()

    const done = shutdown(serverSignal, closeStorageBackend)
    const repeated = shutdown(serverSignal, closeStorageBackend)
    await vi.advanceTimersByTimeAsync(0)
    expect(backend.close).not.toHaveBeenCalled()
    drained.resolve()
    await vi.advanceTimersByTimeAsync(1_999)
    expect(backend.close).toHaveBeenCalledOnce()
    expect(stop).not.toHaveBeenCalled()

    retired.resolve()
    await expect(Promise.all([done, repeated])).resolves.toEqual([undefined, undefined])
    expect(vi.getTimerCount()).toBe(0)
    expect(calls).toEqual([
      'serverSignal.abortAsync',
      'closeStorageBackend',
      'PgTenantConnection.stop',
      'shutdownMultitenantPg',
    ])
  })

  it('bounds queue shutdown by the phase deadline so database teardown follows queue cleanup', async () => {
    vi.useFakeTimers()
    // `pgQueueEnable` is read when the queue module loads: the sync queue needs no engine.
    const { getConfig, mergeConfig } = await import('../config')
    getConfig({ reload: true })
    mergeConfig({ pgQueueEnable: false })
    const { shutdown, closeStorageBackend, calls, stop, logSchema } = await loadShutdown({
      spreadOriginals: true,
    })
    const { AsyncAbortController } = await import('@internal/concurrency')
    const { startQueue } = await import('@internal/queue')
    const { defineQueues } = await import('@internal/queue/manager')

    const root = new AsyncAbortController()
    root.signal.addEventListener(
      'abort',
      () => new Promise<void>((resolve) => setTimeout(resolve, 45_000))
    )
    const queue = await startQueue(
      {
        queues: defineQueues({}),
        handlers: [],
        onStop: async () => {
          calls.push('Queue.onStop')
        },
      },
      { signal: root.nextGroup.signal }
    )
    // A drain that never settles (v1: an in-flight job that never completes).
    const close = vi.spyOn(queue, 'close').mockImplementation(() => new Promise(() => undefined))

    const done = shutdown(root, closeStorageBackend)
    await vi.advanceTimersByTimeAsync(45_000)
    expect(close).toHaveBeenCalledOnce()
    expect(stop).not.toHaveBeenCalled()
    // The phase deadline is 59s in; the queue group only started at 45s, so its stop is
    // bounded to the 14s left rather than its own 25s.
    await vi.advanceTimersByTimeAsync(13_999)
    expect(stop).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(2)

    await expect(done).resolves.toBeUndefined()
    // A queue stop that hits the bound still runs `onStop` before the database teardown.
    expect(calls).toEqual([
      'Queue.onStop',
      'closeStorageBackend',
      'PgTenantConnection.stop',
      'shutdownMultitenantPg',
    ])
    expect(logSchema.error).toHaveBeenCalledWith(
      expect.anything(),
      '[Queue] Error while stopping queue',
      expect.objectContaining({
        type: 'queue',
        error: expect.objectContaining({ message: 'Queue stop timed out after 14000ms' }),
      })
    )
  })
})
