import { vi } from 'vitest'

async function loadShutdown() {
  const calls: string[] = []
  const abortAsync = vi.fn(async () => {
    calls.push('serverSignal.abortAsync')
  })
  const closeStorageBackend = vi.fn(async () => {
    calls.push('closeStorageBackend')
  })
  const stop = vi.fn(async () => {
    calls.push('PgTenantConnection.stop')
  })
  const shutdownMultitenantPg = vi.fn(async () => {
    calls.push('shutdownMultitenantPg')
  })

  vi.doMock('@internal/database', () => ({ shutdownMultitenantPg, PgTenantConnection: { stop } }))
  vi.doMock('../http/plugins/storage', () => ({ closeStorageBackend }))
  vi.doMock('@internal/monitoring', () => ({
    logger: { flush: vi.fn() },
    logSchema: { error: vi.fn(), info: vi.fn() },
  }))

  const { shutdown } = await import('./shutdown')
  const serverSignal = { abortAsync } as unknown as Parameters<typeof shutdown>[0]
  return { shutdown, serverSignal, abortAsync, closeStorageBackend, stop, calls }
}

describe('shutdown', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.doUnmock('@internal/database')
    vi.doUnmock('@internal/monitoring')
    vi.doUnmock('../http/plugins/storage')
    vi.resetModules()
  })

  it('drains HTTP users and awaits storage closure before database teardown', async () => {
    const { shutdown, serverSignal, abortAsync, closeStorageBackend, stop, calls } =
      await loadShutdown()
    const drained = Promise.withResolvers<void>()
    const closed = Promise.withResolvers<void>()
    abortAsync.mockImplementation(async () => {
      calls.push('serverSignal.abortAsync')
      await drained.promise
    })
    closeStorageBackend.mockImplementation(async () => {
      calls.push('closeStorageBackend')
      await closed.promise
    })

    const stopping = shutdown(serverSignal)
    const repeated = shutdown(serverSignal)
    expect(closeStorageBackend).not.toHaveBeenCalled()
    drained.resolve()
    await vi.waitFor(() => expect(closeStorageBackend).toHaveBeenCalledOnce())
    expect(stop).not.toHaveBeenCalled()
    closed.resolve()
    await Promise.all([stopping, repeated])

    expect(calls).toEqual([
      'serverSignal.abortAsync',
      'closeStorageBackend',
      'PgTenantConnection.stop',
      'shutdownMultitenantPg',
    ])
  })

  it('preserves the shared backend if HTTP shutdown does not settle', async () => {
    vi.useFakeTimers()
    const { shutdown, serverSignal, abortAsync, closeStorageBackend, calls } = await loadShutdown()
    abortAsync.mockImplementation(() => {
      calls.push('serverSignal.abortAsync')
      return new Promise<void>(() => undefined)
    })

    const failure = shutdown(serverSignal).catch((error) => error)
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

  it.each([
    'rejects',
    'times out',
  ])('continues database teardown when storage closure %s', async (mode) => {
    if (mode === 'times out') vi.useFakeTimers()
    const { shutdown, serverSignal, closeStorageBackend, calls } = await loadShutdown()
    const closeError = new Error('storage close failed')
    closeStorageBackend.mockImplementation(async () => {
      calls.push('closeStorageBackend')
      if (mode === 'rejects') throw closeError
      await new Promise<void>(() => undefined)
    })

    const failure = shutdown(serverSignal).catch((error) => error)
    if (mode === 'times out') await vi.advanceTimersByTimeAsync(60_000)

    if (mode === 'rejects') expect(await failure).toBe(closeError)
    else
      expect(await failure).toEqual(
        expect.objectContaining({
          message: 'Shutdown phase "close storage backend" timed out after 60000ms',
        })
      )
    expect(calls).toEqual([
      'serverSignal.abortAsync',
      'closeStorageBackend',
      'PgTenantConnection.stop',
      'shutdownMultitenantPg',
    ])
  })

  it('rejects when tenant pool drain fails after closing the shared pool', async () => {
    const { shutdown, serverSignal, stop, calls } = await loadShutdown()
    const tenantError = new Error('tenant pool drain failed')
    stop.mockImplementation(async () => {
      calls.push('PgTenantConnection.stop')
      throw tenantError
    })

    await expect(shutdown(serverSignal)).rejects.toBe(tenantError)
    expect(calls).toEqual([
      'serverSignal.abortAsync',
      'closeStorageBackend',
      'PgTenantConnection.stop',
      'shutdownMultitenantPg',
    ])
  })
})
