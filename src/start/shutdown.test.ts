import { vi } from 'vitest'

describe('shutdown', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.doUnmock('@internal/database')
    vi.doUnmock('@internal/monitoring')
    vi.resetModules()
  })

  it('drains tenant pools before closing the shared multitenant pool', async () => {
    const calls: string[] = []
    const closeMultitenantPg = vi.fn(async () => {
      calls.push('closeMultitenantPg')
    })
    const stop = vi.fn(async () => {
      calls.push('PgTenantConnection.stop')
    })

    vi.doMock('@internal/database', () => ({
      closeMultitenantPg,
      PgTenantConnection: {
        stop,
      },
    }))
    vi.doMock('@internal/monitoring', () => ({
      logger: {
        flush: vi.fn(),
      },
      logSchema: {
        error: vi.fn(),
        info: vi.fn(),
      },
    }))

    const { shutdown } = await import('./shutdown')
    const serverSignal = {
      abortAsync: vi.fn(async () => {
        calls.push('serverSignal.abortAsync')
      }),
    } as unknown as Parameters<typeof shutdown>[0]

    await shutdown(serverSignal)

    expect(calls).toEqual([
      'serverSignal.abortAsync',
      'PgTenantConnection.stop',
      'closeMultitenantPg',
    ])
  })

  it('continues to pool teardown when server signal abort does not settle', async () => {
    vi.useFakeTimers()

    const calls: string[] = []
    const closeMultitenantPg = vi.fn(async () => {
      calls.push('closeMultitenantPg')
    })
    const stop = vi.fn(async () => {
      calls.push('PgTenantConnection.stop')
    })

    vi.doMock('@internal/database', () => ({
      closeMultitenantPg,
      PgTenantConnection: {
        stop,
      },
    }))
    vi.doMock('@internal/monitoring', () => ({
      logger: {
        flush: vi.fn(),
      },
      logSchema: {
        error: vi.fn(),
        info: vi.fn(),
      },
    }))

    const { shutdown } = await import('./shutdown')
    const serverSignal = {
      abortAsync: vi.fn(() => {
        calls.push('serverSignal.abortAsync')
        return new Promise<void>(() => undefined)
      }),
    } as unknown as Parameters<typeof shutdown>[0]

    const shutdownPromise = shutdown(serverSignal)
    const shutdownErrorPromise = shutdownPromise.catch((error) => error)

    await vi.advanceTimersByTimeAsync(60_000)

    expect(await shutdownErrorPromise).toEqual(
      expect.objectContaining({
        message: 'Shutdown phase "abort server signal" timed out after 60000ms',
      })
    )
    expect(calls).toEqual([
      'serverSignal.abortAsync',
      'PgTenantConnection.stop',
      'closeMultitenantPg',
    ])
  })

  it('rejects when tenant pool drain fails after closing the shared pool', async () => {
    const calls: string[] = []
    const tenantError = new Error('tenant pool drain failed')
    const closeMultitenantPg = vi.fn(async () => {
      calls.push('closeMultitenantPg')
    })
    const stop = vi.fn(async () => {
      calls.push('PgTenantConnection.stop')
      throw tenantError
    })

    vi.doMock('@internal/database', () => ({
      closeMultitenantPg,
      PgTenantConnection: {
        stop,
      },
    }))
    vi.doMock('@internal/monitoring', () => ({
      logger: {
        flush: vi.fn(),
      },
      logSchema: {
        error: vi.fn(),
        info: vi.fn(),
      },
    }))

    const { shutdown } = await import('./shutdown')
    const serverSignal = {
      abortAsync: vi.fn(async () => {
        calls.push('serverSignal.abortAsync')
      }),
    } as unknown as Parameters<typeof shutdown>[0]

    await expect(shutdown(serverSignal)).rejects.toBe(tenantError)
    expect(calls).toEqual([
      'serverSignal.abortAsync',
      'PgTenantConnection.stop',
      'closeMultitenantPg',
    ])
  })
})
