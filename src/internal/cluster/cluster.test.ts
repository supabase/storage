import { getEventListeners } from 'node:events'
import { logger } from '@internal/monitoring'
import { vi } from 'vitest'

const { ecsLoaded, eksLoaded, getEcsClusterSize, getEksClusterSize, config } = vi.hoisted(() => ({
  ecsLoaded: vi.fn(),
  eksLoaded: vi.fn(),
  getEcsClusterSize: vi.fn((_signal: AbortSignal) => Promise.resolve(2)),
  getEksClusterSize: vi.fn((_signal: AbortSignal) => Promise.resolve(3)),
  config: {
    clusterDiscoveryTimeoutMs: 30_000,
    clusterDiscoveryPollIntervalMs: 20_000,
    clusterDiscoveryEcsMaxRps: 10,
    numWorkers: 1,
  },
}))

vi.mock('../../config', () => ({ getConfig: () => config }))

vi.mock('@internal/monitoring', () => ({
  logger: {
    info: vi.fn(),
  },
}))

vi.mock('./ecs', () => {
  ecsLoaded()

  return {
    ClusterDiscoveryECS: class {
      getClusterSize(signal: AbortSignal) {
        return getEcsClusterSize(signal)
      }
    },
  }
})

vi.mock('./eks', () => {
  eksLoaded()

  return {
    ClusterDiscoveryEKS: class {
      getClusterSize(signal: AbortSignal) {
        return getEksClusterSize(signal)
      }
    },
  }
})

describe('Cluster', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    vi.resetModules()
    getEcsClusterSize.mockReset()
    getEksClusterSize.mockReset()
    Object.assign(config, {
      clusterDiscoveryTimeoutMs: 30_000,
      clusterDiscoveryPollIntervalMs: 20_000,
      clusterDiscoveryEcsMaxRps: 10,
      numWorkers: 1,
    })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it.each([
    { discovery: 'ECS', size: 2, selected: ecsLoaded, skipped: eksLoaded },
    { discovery: 'EKS', size: 3, selected: eksLoaded, skipped: ecsLoaded },
  ])('loads only the $discovery discovery implementation', async ({
    discovery,
    size,
    selected,
    skipped,
  }) => {
    vi.stubEnv('CLUSTER_DISCOVERY', discovery)

    const { Cluster } = await import('./cluster')
    const abortController = new AbortController()

    try {
      await Cluster.init(abortController.signal)

      expect(Cluster.size).toBe(size)
      expect(selected).toHaveBeenCalledTimes(1)
      expect(skipped).not.toHaveBeenCalled()
    } finally {
      abortController.abort()
    }
  })

  it('does not load a discovery implementation when CLUSTER_DISCOVERY is unset', async () => {
    vi.stubEnv('CLUSTER_DISCOVERY', undefined)

    const { Cluster } = await import('./cluster')

    await Cluster.init(new AbortController().signal)

    expect(Cluster.size).toBe(0)
    expect(ecsLoaded).not.toHaveBeenCalled()
    expect(eksLoaded).not.toHaveBeenCalled()
  })

  it('does not initialize discovery when the abort signal is already aborted', async () => {
    vi.stubEnv('CLUSTER_DISCOVERY', 'ECS')

    const { Cluster } = await import('./cluster')
    const abortController = new AbortController()
    abortController.abort()

    await Cluster.init(abortController.signal)

    expect(Cluster.size).toBe(0)
    expect(ecsLoaded).not.toHaveBeenCalled()
    expect(eksLoaded).not.toHaveBeenCalled()
  })

  it.each([
    { discovery: 'ECS', getSize: getEcsClusterSize },
    { discovery: 'EKS', getSize: getEksClusterSize },
  ])('stops initialization when aborted while loading $discovery discovery', async ({
    discovery,
    getSize,
  }) => {
    vi.stubEnv('CLUSTER_DISCOVERY', discovery)

    const { Cluster } = await import('./cluster')
    const abortController = new AbortController()

    const initialization = Cluster.init(abortController.signal)
    abortController.abort()
    await initialization

    expect(Cluster.size).toBe(0)
    expect(getSize).not.toHaveBeenCalled()
  })

  it.each([
    'during discovery',
    'before publication',
    'before rejection',
  ])('does not publish or watch when initial discovery is aborted %s', async (timing) => {
    vi.stubEnv('CLUSTER_DISCOVERY', 'ECS')

    const { Cluster } = await import('./cluster')
    const abortController = new AbortController()
    getEcsClusterSize.mockImplementationOnce(() => {
      if (timing !== 'during discovery') {
        queueMicrotask(() => queueMicrotask(() => queueMicrotask(() => abortController.abort())))
      } else {
        abortController.abort()
      }
      return timing === 'before rejection' ? Promise.reject(new Error('late')) : Promise.resolve(2)
    })

    await Cluster.init(abortController.signal)

    expect(Cluster.size).toBe(0)
    expect(getEcsClusterSize).toHaveBeenCalledOnce()
    expect(vi.getTimerCount()).toBe(0)
  })

  it('does not watch when aborted while logging the initial size', async () => {
    vi.stubEnv('CLUSTER_DISCOVERY', 'ECS')
    const { Cluster } = await import('./cluster')
    const abortController = new AbortController()
    getEcsClusterSize.mockResolvedValueOnce(2)
    vi.mocked(logger.info).mockImplementationOnce(() => abortController.abort())
    await Cluster.init(abortController.signal)

    expect(vi.getTimerCount()).toBe(0)
  })

  it('propagates an initial discovery failure when not shutting down', async () => {
    vi.stubEnv('CLUSTER_DISCOVERY', 'ECS')
    const { Cluster } = await import('./cluster')
    const error = new Error('discovery failed')
    getEcsClusterSize.mockRejectedValueOnce(error)

    await expect(Cluster.init(new AbortController().signal)).rejects.toBe(error)
    expect(Cluster.size).toBe(0)
  })

  it('allows a startup listing longer than 30 seconds with a larger configured deadline', async () => {
    vi.stubEnv('CLUSTER_DISCOVERY', 'ECS')
    config.clusterDiscoveryTimeoutMs = 45_000
    const { Cluster } = await import('./cluster')
    const shutdown = new AbortController()
    const discovery = Promise.withResolvers<number>()
    getEcsClusterSize.mockReturnValueOnce(discovery.promise)
    const initialization = Cluster.init(shutdown.signal)

    await vi.advanceTimersByTimeAsync(35_000)
    expect(Cluster.size).toBe(0)
    discovery.resolve(2)
    await initialization
    expect(Cluster.size).toBe(2)
    shutdown.abort()
  })

  describe('watcher', () => {
    const POLL_MS = 20_000

    async function start(initialSize: number, discovery = 'ECS') {
      vi.stubEnv('CLUSTER_DISCOVERY', discovery)
      const getSize = discovery === 'ECS' ? getEcsClusterSize : getEksClusterSize
      getSize.mockResolvedValueOnce(initialSize)

      const { Cluster } = await import('./cluster')
      const abortController = new AbortController()
      await Cluster.init(abortController.signal)
      getSize.mockClear()

      return { Cluster, abortController }
    }

    it('does not start a poll while one is pending and reschedules after it settles', async () => {
      const { abortController } = await start(2)
      const pending = Promise.withResolvers<number>()
      getEcsClusterSize.mockImplementationOnce(() => pending.promise)

      await vi.advanceTimersByTimeAsync(POLL_MS * 2)
      expect(getEcsClusterSize).toHaveBeenCalledTimes(1)

      pending.resolve(2)
      await vi.advanceTimersByTimeAsync(POLL_MS - 1)
      expect(getEcsClusterSize).toHaveBeenCalledTimes(1)
      await vi.advanceTimersByTimeAsync(1)
      expect(getEcsClusterSize).toHaveBeenCalledTimes(2)

      abortController.abort()
    })

    it.each([
      { base: 45_000, ms: 45_000 },
      { size: 0, random: 0, ms: 10_000 },
      { size: 250, random: 0, ms: 37_500 },
      { size: 250, random: 0.999, ms: 112_425 },
      { size: 200, workers: 2, rps: 5, ms: 160_000 },
      { size: 200, workers: 4, ms: 160_000 },
      { size: 0, workers: 0, rps: 1, base: 100, ms: 1_000 },
      { discovery: 'EKS', size: 250, workers: 4, ms: POLL_MS },
      { base: 2 ** 31 - 1, random: 0.999, ms: 2 ** 31 - 1 },
    ])('polls again after $ms ms', async ({
      discovery = 'ECS',
      size = 2,
      workers = 1,
      rps = 10,
      base = POLL_MS,
      random = 0.5,
      ms,
    }) => {
      vi.spyOn(Math, 'random').mockReturnValue(random)
      Object.assign(config, {
        numWorkers: workers,
        clusterDiscoveryEcsMaxRps: rps,
        clusterDiscoveryPollIntervalMs: base,
      })
      const getSize = discovery === 'ECS' ? getEcsClusterSize : getEksClusterSize
      const { abortController } = await start(size, discovery)
      getSize.mockResolvedValue(size)

      await vi.advanceTimersByTimeAsync(ms - 1)
      expect(getSize).not.toHaveBeenCalled()
      await vi.advanceTimersByTimeAsync(1)
      expect(getSize).toHaveBeenCalledOnce()
      abortController.abort()
    })

    it('backs off failed polls, caps the backoff and resets after success', async () => {
      vi.spyOn(console, 'error').mockImplementation(() => {})
      const { abortController } = await start(2)
      getEcsClusterSize.mockRejectedValue(new Error('throttled'))

      await vi.advanceTimersByTimeAsync(POLL_MS)
      expect(getEcsClusterSize).toHaveBeenCalledTimes(1)

      for (const [index, delay] of [40_000, 80_000, 160_000, 300_000, 300_000].entries()) {
        await vi.advanceTimersByTimeAsync(delay - 1)
        expect(getEcsClusterSize).toHaveBeenCalledTimes(index + 1)
        await vi.advanceTimersByTimeAsync(1)
        expect(getEcsClusterSize).toHaveBeenCalledTimes(index + 2)
      }

      getEcsClusterSize.mockResolvedValue(2)
      await vi.advanceTimersByTimeAsync(300_000)
      expect(getEcsClusterSize).toHaveBeenCalledTimes(7)
      await vi.advanceTimersByTimeAsync(POLL_MS - 1)
      expect(getEcsClusterSize).toHaveBeenCalledTimes(7)
      await vi.advanceTimersByTimeAsync(1)
      expect(getEcsClusterSize).toHaveBeenCalledTimes(8)

      abortController.abort()
    })

    it('does not shorten a fleet interval longer than the failure backoff cap', async () => {
      vi.spyOn(console, 'error').mockImplementation(() => {})
      const { abortController } = await start(1_000)
      getEcsClusterSize.mockRejectedValue(new Error('throttled'))
      await vi.advanceTimersByTimeAsync(1_000_000 - 1)
      expect(getEcsClusterSize).not.toHaveBeenCalled()
      await vi.advanceTimersByTimeAsync(1)
      expect(getEcsClusterSize).toHaveBeenCalledOnce()
      await vi.advanceTimersByTimeAsync(999_999)
      expect(getEcsClusterSize).toHaveBeenCalledOnce()
      await vi.advanceTimersByTimeAsync(1)
      expect(getEcsClusterSize).toHaveBeenCalledTimes(2)
      abortController.abort()
    })

    it('emits change only for a truthy different size and polls next with the new size', async () => {
      const { Cluster, abortController } = await start(2)
      const listener = vi.fn()
      Cluster.on('change', listener)
      getEcsClusterSize.mockResolvedValueOnce(0)

      await vi.advanceTimersByTimeAsync(POLL_MS)
      expect(listener).not.toHaveBeenCalled()
      expect(Cluster.size).toBe(2)

      getEcsClusterSize.mockResolvedValueOnce(250)
      await vi.advanceTimersByTimeAsync(POLL_MS)
      expect(listener).toHaveBeenCalledWith({ size: 250 })
      expect(Cluster.size).toBe(250)

      await vi.advanceTimersByTimeAsync(75_000 - 1)
      expect(getEcsClusterSize).toHaveBeenCalledTimes(2)
      await vi.advanceTimersByTimeAsync(1)
      expect(getEcsClusterSize).toHaveBeenCalledTimes(3)

      abortController.abort()
    })

    it.each([
      { outcome: 'resolves', discover: () => Promise.resolve(250) },
      { outcome: 'rejects', discover: () => Promise.reject(new Error('late')) },
      { outcome: 'hangs', discover: () => new Promise<number>(() => {}) },
      { outcome: 'resolves before shutdown', discover: () => Promise.resolve(250), late: true },
      {
        outcome: 'rejects before shutdown',
        discover: () => Promise.reject(new Error('late')),
        late: true,
      },
    ])('does not publish, log or reschedule when a poll that $outcome is aborted', async ({
      discover,
      late = false,
    }) => {
      const error = vi.spyOn(console, 'error').mockImplementation(() => {})
      const { Cluster, abortController } = await start(2)
      const listener = vi.fn()
      Cluster.on('change', listener)
      getEcsClusterSize.mockImplementationOnce(() => {
        if (late) {
          queueMicrotask(() => queueMicrotask(() => queueMicrotask(() => abortController.abort())))
        } else {
          abortController.abort()
        }
        return discover()
      })

      await vi.advanceTimersByTimeAsync(POLL_MS)

      expect(getEcsClusterSize).toHaveBeenCalledOnce()
      expect(Cluster.size).toBe(2)
      expect(listener).not.toHaveBeenCalled()
      expect(error).not.toHaveBeenCalled()
      expect(vi.getTimerCount()).toBe(0)
    })

    it('keeps the discovered size and normal cadence when a change listener throws', async () => {
      const error = new Error('listener failed')
      const logError = vi.spyOn(console, 'error').mockImplementation(() => {})
      const { Cluster, abortController } = await start(2)
      const listener = vi.fn(() => {
        expect(Cluster.size).toBe(3)
        throw error
      })
      Cluster.on('change', listener)
      getEcsClusterSize.mockResolvedValue(3)

      await vi.advanceTimersByTimeAsync(POLL_MS)
      expect(Cluster.size).toBe(3)
      expect(listener).toHaveBeenCalledOnce()
      await vi.advanceTimersByTimeAsync(POLL_MS - 1)
      expect(getEcsClusterSize).toHaveBeenCalledOnce()
      await vi.advanceTimersByTimeAsync(1)
      expect(getEcsClusterSize).toHaveBeenCalledTimes(2)
      expect(listener).toHaveBeenCalledOnce()
      expect(logError).toHaveBeenCalledWith('Error notifying cluster size change', error)
      abortController.abort()
    })

    it('does not reschedule after a change listener aborts', async () => {
      const { Cluster, abortController } = await start(2)
      Cluster.on('change', () => abortController.abort())
      getEcsClusterSize.mockResolvedValueOnce(250)

      await vi.advanceTimersByTimeAsync(POLL_MS)

      expect(Cluster.size).toBe(250)
      expect(vi.getTimerCount()).toBe(0)
    })

    it('clears the pending timer and listeners when aborted while idle', async () => {
      const { Cluster, abortController } = await start(2)
      expect(vi.getTimerCount()).toBe(1)
      expect(getEventListeners(abortController.signal, 'abort')).toHaveLength(1)
      const listener = vi.fn()
      Cluster.on('change', listener)

      abortController.abort()
      expect(vi.getTimerCount()).toBe(0)

      getEcsClusterSize.mockResolvedValueOnce(2).mockResolvedValueOnce(250)
      const next = new AbortController()
      await Cluster.init(next.signal)
      await vi.advanceTimersByTimeAsync(POLL_MS)
      expect(listener).not.toHaveBeenCalled()
      next.abort()
    })

    describe('deadline', () => {
      const TIMEOUT_MS = 5_000

      beforeEach(() => {
        config.clusterDiscoveryTimeoutMs = TIMEOUT_MS
      })

      it.each([
        { trigger: 'deadline', elapsed: TIMEOUT_MS - 1 },
        { trigger: 'shutdown', elapsed: 1 },
      ])('cancels hung initial discovery on $trigger', async ({ trigger, elapsed }) => {
        vi.stubEnv('CLUSTER_DISCOVERY', 'ECS')
        const { Cluster } = await import('./cluster')
        const shutdown = new AbortController()
        let signal!: AbortSignal
        getEcsClusterSize.mockImplementationOnce((s) => {
          signal = s
          return new Promise<number>(() => {})
        })
        const result = Cluster.init(shutdown.signal).catch((e) => e)

        await vi.advanceTimersByTimeAsync(elapsed)
        expect(signal.aborted).toBe(false)
        if (trigger === 'shutdown') {
          shutdown.abort()
          expect(await result).toBeUndefined()
        } else {
          await vi.advanceTimersByTimeAsync(1)
          expect(await result).toMatchObject({ name: 'TimeoutError' })
        }
        expect(signal.reason).toMatchObject({ name: 'AbortError' })
        expect(Cluster.size).toBe(0)
        expect(vi.getTimerCount()).toBe(0)
        expect(getEventListeners(shutdown.signal, 'abort')).toHaveLength(0)
      })

      it('rejects a hung EKS poll at the deadline, logs it and reschedules', async () => {
        const error = vi.spyOn(console, 'error').mockImplementation(() => {})
        const { abortController } = await start(3, 'EKS')
        getEksClusterSize.mockImplementationOnce(() => new Promise<number>(() => {}))

        await vi.advanceTimersByTimeAsync(POLL_MS + TIMEOUT_MS)

        expect(error).toHaveBeenCalledWith(
          'Error getting cluster size',
          expect.objectContaining({ name: 'TimeoutError' })
        )
        expect(vi.getTimerCount()).toBe(1)
        getEksClusterSize.mockResolvedValue(3)
        await vi.advanceTimersByTimeAsync(2 * POLL_MS - 1)
        expect(getEksClusterSize).toHaveBeenCalledOnce()
        await vi.advanceTimersByTimeAsync(1)
        expect(getEksClusterSize).toHaveBeenCalledTimes(2)
        abortController.abort()
      })
    })
  })
})
