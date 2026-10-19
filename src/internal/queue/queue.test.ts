import { vi } from 'vitest'

type QueueModule = typeof import('./queue')
type MonitoringModule = typeof import('../monitoring')

async function loadQueueModule() {
  vi.resetModules()

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })

  const queueModule = (await import('./queue')) as QueueModule
  const monitoringModule = (await import('../monitoring')) as MonitoringModule

  return { queueModule, monitoringModule }
}

const createJob = (id: string, data: Record<string, unknown> = {}) => ({
  id,
  data,
  priority: 1,
  retryCount: 0,
  retryLimit: 1,
})

describe('Queue worker', () => {
  const createBoss = (...ids: string[]) => ({
    on: vi.fn(),
    start: vi.fn().mockResolvedValue(undefined),
    createQueue: vi.fn().mockResolvedValue(undefined),
    fetch: vi
      .fn()
      .mockResolvedValueOnce(ids.map((id) => createJob(id)))
      .mockResolvedValue([]),
    complete: vi.fn().mockResolvedValue(undefined),
    stop: vi.fn().mockResolvedValue(undefined),
  })

  function registerWorker(
    Queue: QueueModule['Queue'],
    handle: (job: { id: string }) => Promise<unknown>
  ) {
    Queue.register({
      name: 'TestEvent',
      getQueueName: () => 'test-queue',
      deadLetterQueueName: () => 'test-queue-dlq',
      getQueueOptions: () => ({}),
      getWorkerOptions: () => ({ pollingIntervalSeconds: 1, concurrentTaskCount: 1 }),
      handle,
      onStart: () => undefined,
      onClose: () => undefined,
    } as unknown as Parameters<typeof Queue.register>[0])
  }

  async function startWorker(opts: {
    handle: (job: { id: string; data: Record<string, unknown> }) => Promise<unknown>
    fetch?: ReturnType<typeof vi.fn>
    complete?: ReturnType<typeof vi.fn>
    fail?: ReturnType<typeof vi.fn>
    executeSql?: ReturnType<typeof vi.fn>
    workerOptions?: Record<string, unknown>
    concurrentTaskCount?: number
    onMessage?: (job: { data: Record<string, unknown> }) => void
  }) {
    const {
      queueModule: { Queue },
      monitoringModule,
    } = await loadQueueModule()
    const abortController = new AbortController()
    const errorSpy = vi.spyOn(monitoringModule.logSchema, 'error')
    const complete = opts.complete ?? vi.fn().mockResolvedValue(undefined)
    const fail = opts.fail ?? vi.fn().mockResolvedValue(undefined)
    const fetch =
      opts.fetch ??
      vi
        .fn()
        .mockResolvedValueOnce([createJob('job-1')])
        .mockResolvedValueOnce([])
    const stop = vi.fn().mockResolvedValue(undefined)
    const executeSql = opts.executeSql ?? vi.fn().mockResolvedValue({ rows: [] })

    ;(Queue as unknown as { pgBoss: unknown }).pgBoss = { complete, fail, fetch, stop }
    ;(Queue as unknown as { pgBossDb: unknown }).pgBossDb = { executeSql }
    ;(Queue as unknown as { pollQueue: Function }).pollQueue(
      {
        getQueueName: () => 'test-queue',
        getWorkerOptions: () => ({
          pollingIntervalSeconds: 1,
          ...opts.workerOptions,
        }),
        handle: opts.handle,
        name: 'TestEvent',
      },
      {
        concurrentTaskCount: opts.concurrentTaskCount ?? 1,
        onMessage: opts.onMessage,
        signal: abortController.signal,
      }
    )

    return {
      Queue,
      monitoringModule,
      abortController,
      errorSpy,
      complete,
      fail,
      fetch,
      stop,
      executeSql,
    }
  }

  afterEach(() => {
    vi.useRealTimers()
    vi.resetModules()
  })

  it.each([
    ['top-level sbReqId', { sbReqId: 'sb-req-123', reqId: 'trace-123' }, 'sb-req-123'],
    [
      'nested webhook sbReqId',
      {
        event: {
          payload: {
            sbReqId: 'sb-req-456',
            reqId: 'trace-456',
          },
        },
      },
      'sb-req-456',
    ],
    [
      'missing sbReqId',
      {
        reqId: 'trace-only',
      },
      undefined,
    ],
  ])('propagates %s into queue job handling', async (_label, data, expectedSbReqId) => {
    vi.useFakeTimers()

    let onMessageSbReqId: string | undefined
    const { monitoringModule, abortController, errorSpy } = await startWorker({
      fetch: vi
        .fn()
        .mockResolvedValueOnce([createJob('job-1', data)])
        .mockResolvedValueOnce([]),
      handle: vi.fn(async () => {
        throw new Error('boom')
      }),
      onMessage: (job) => {
        const payload = job.data as {
          sbReqId?: string
          event?: { payload?: { sbReqId?: string } }
        }
        onMessageSbReqId = payload.sbReqId ?? payload.event?.payload?.sbReqId
      },
    })

    await vi.advanceTimersByTimeAsync(1_000)
    abortController.abort()

    expect(onMessageSbReqId).toBe(expectedSbReqId)
    const processingErrorCall = errorSpy.mock.calls.find(
      ([, message]) => message === '[Queue Handler] Error while processing job TestEvent'
    )
    expect(processingErrorCall?.[0]).toBe(monitoringModule.logger)
    expect((processingErrorCall?.[2] as { sbReqId?: string })?.sbReqId).toBe(expectedSbReqId)
  })

  it('limits concurrent handlers and fetches remaining capacity while jobs are active', async () => {
    vi.useFakeTimers()

    const releaseJob1 = Promise.withResolvers<void>()
    const releaseJob2 = Promise.withResolvers<void>()
    const releaseJob3 = Promise.withResolvers<void>()
    const releases = new Map([
      ['job-1', releaseJob1],
      ['job-2', releaseJob2],
      ['job-3', releaseJob3],
    ])
    const startedJobs: string[] = []
    let activeHandlers = 0
    let maxActiveHandlers = 0

    const { abortController, complete, fetch } = await startWorker({
      fetch: vi
        .fn()
        .mockResolvedValueOnce([createJob('job-1'), createJob('job-2')])
        .mockResolvedValueOnce([createJob('job-3')])
        .mockResolvedValueOnce([]),
      handle: vi.fn(async (job: { id: string }) => {
        startedJobs.push(job.id)
        activeHandlers++
        maxActiveHandlers = Math.max(maxActiveHandlers, activeHandlers)

        try {
          await releases.get(job.id)?.promise
        } finally {
          activeHandlers--
        }
      }),
      workerOptions: { batchSize: 3 },
      concurrentTaskCount: 2,
    })

    await vi.advanceTimersByTimeAsync(1_000)

    expect(fetch).toHaveBeenNthCalledWith(
      1,
      'test-queue',
      expect.objectContaining({ batchSize: 3 })
    )
    expect(startedJobs).toEqual(['job-1', 'job-2'])
    expect(maxActiveHandlers).toBe(2)

    await vi.advanceTimersByTimeAsync(1_000)

    expect(fetch).toHaveBeenNthCalledWith(
      2,
      'test-queue',
      expect.objectContaining({ batchSize: 1 })
    )
    expect(startedJobs).toEqual(['job-1', 'job-2'])

    releaseJob1.resolve()
    await vi.advanceTimersByTimeAsync(0)

    expect(startedJobs).toEqual(['job-1', 'job-2', 'job-3'])
    expect(maxActiveHandlers).toBe(2)

    releaseJob2.resolve()
    releaseJob3.resolve()
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(1_000)

    expect(fetch).toHaveBeenNthCalledWith(
      3,
      'test-queue',
      expect.objectContaining({ batchSize: 3 })
    )
    expect(complete).toHaveBeenCalledTimes(3)

    abortController.abort()
  })

  it('immediately releases waiting jobs across batches when shutdown begins', async () => {
    vi.useFakeTimers()
    const running = Promise.withResolvers<void>()
    const handle = vi.fn(() => running.promise)
    const { Queue, abortController, complete, fail, executeSql } = await startWorker({
      fetch: vi
        .fn()
        .mockResolvedValueOnce([createJob('job-1'), createJob('job-2')])
        .mockResolvedValueOnce([createJob('job-3')])
        .mockResolvedValue([]),
      workerOptions: { batchSize: 3 },
      handle,
    })

    await vi.advanceTimersByTimeAsync(2_000)
    expect(handle).toHaveBeenCalledOnce()
    abortController.abort()
    const stopping = Queue.stop()
    await vi.advanceTimersByTimeAsync(0)
    expect(executeSql).toHaveBeenCalledExactlyOnceWith(expect.any(String), [
      'test-queue',
      ['job-2', 'job-3'],
    ])
    expect(complete).not.toHaveBeenCalled()

    running.resolve()
    await stopping

    expect(handle).toHaveBeenCalledOnce()
    expect(complete).toHaveBeenCalledExactlyOnceWith('test-queue', 'job-1')
    expect(fail).not.toHaveBeenCalled()
    expect(executeSql).toHaveBeenCalledOnce()
  })

  it.each(['before', 'after'] as const)(
    'claims each waiting job once when abort happens %s its slot callback starts',
    async (timing) => {
      vi.useFakeTimers()
      const first = Promise.withResolvers<void>()
      const second = Promise.withResolvers<void>()
      const handle = vi.fn((job: { id: string }) =>
        job.id === 'job-1' ? first.promise : second.promise
      )
      const { Queue, abortController, executeSql, complete } = await startWorker({
        fetch: vi.fn().mockResolvedValue([createJob('job-1'), createJob('job-2')]),
        handle,
      })

      await vi.advanceTimersByTimeAsync(1_000)
      first.resolve()
      if (timing === 'after') await vi.advanceTimersByTimeAsync(0)
      abortController.abort()
      const stopping = Queue.stop()
      await vi.advanceTimersByTimeAsync(0)
      second.resolve()
      await stopping

      if (timing === 'before') {
        expect(handle).toHaveBeenCalledOnce()
        expect(executeSql).toHaveBeenCalledExactlyOnceWith(expect.any(String), [
          'test-queue',
          ['job-2'],
        ])
        expect(complete).toHaveBeenCalledExactlyOnceWith('test-queue', 'job-1')
      } else {
        expect(handle).toHaveBeenCalledTimes(2)
        expect(executeSql).not.toHaveBeenCalled()
        expect(complete).toHaveBeenCalledWith('test-queue', 'job-2')
      }
    }
  )

  it('releases waiting jobs through the Queue.start wiring before pg-boss stops', async () => {
    vi.useFakeTimers()
    const {
      queueModule: { Queue },
    } = await loadQueueModule()
    const { AsyncAbortController } = await import('../concurrency')
    const running = Promise.withResolvers<void>()
    const released = Promise.withResolvers<{ rows: [] }>()
    const executeSql = vi.fn(() => released.promise)
    const boss = createBoss('job-1', 'job-2')
    vi.spyOn(Queue, 'createPgBoss').mockReturnValue(
      boss as unknown as ReturnType<typeof Queue.createPgBoss>
    )
    const controller = new AsyncAbortController()
    await Queue.start({
      signal: controller.signal,
      registerWorkers: () => registerWorker(Queue, () => running.promise),
    })
    ;(Queue as unknown as { pgBossDb: unknown }).pgBossDb = { executeSql }
    await vi.advanceTimersByTimeAsync(1_000)

    const stopping = controller.abortAsync()
    running.resolve()
    await vi.advanceTimersByTimeAsync(0)
    expect(executeSql).toHaveBeenCalledExactlyOnceWith(expect.any(String), [
      'test-queue',
      ['job-2'],
    ])
    expect(boss.stop).not.toHaveBeenCalled()

    released.resolve({ rows: [] })
    await stopping
    expect(boss.stop).toHaveBeenCalledOnce()
  })

  it.each([false, true])(
    'drains a restarted queue independently of old jobs (late completion: %s)',
    async (completeOld) => {
      vi.useFakeTimers()
      const {
        queueModule: { Queue },
      } = await loadQueueModule()
      const { AsyncAbortController } = await import('../concurrency')
      const oldJob = Promise.withResolvers<void>()
      const newJob = Promise.withResolvers<void>()
      const firstBoss = createBoss('old')
      const nextBoss = createBoss('new')
      vi.spyOn(Queue, 'createPgBoss')
        .mockReturnValueOnce(firstBoss as unknown as ReturnType<typeof Queue.createPgBoss>)
        .mockReturnValueOnce(nextBoss as unknown as ReturnType<typeof Queue.createPgBoss>)
      const first = new AsyncAbortController()
      await Queue.start({
        signal: first.signal,
        registerWorkers: () =>
          registerWorker(Queue, (job) => (job.id === 'old' ? oldJob.promise : newJob.promise)),
      })
      await vi.advanceTimersByTimeAsync(1_000)
      const firstStop = first.abortAsync()
      await vi.advanceTimersByTimeAsync(25_000)
      await firstStop
      expect(firstBoss.stop).toHaveBeenCalledOnce()

      const next = new AsyncAbortController()
      await Queue.start({ signal: next.signal })
      await vi.advanceTimersByTimeAsync(1_000)
      const nextStop = next.abortAsync()
      if (completeOld) oldJob.resolve()
      await vi.advanceTimersByTimeAsync(0)
      expect(nextBoss.stop).not.toHaveBeenCalled()

      newJob.resolve()
      await vi.advanceTimersByTimeAsync(0)
      expect(nextBoss.stop).toHaveBeenCalledOnce()
      await nextStop
      oldJob.resolve()
      await vi.advanceTimersByTimeAsync(0)
    }
  )

  it('logs a release failure without running or completing the waiting job', async () => {
    vi.useFakeTimers()
    const running = Promise.withResolvers<void>()
    const handle = vi.fn(() => running.promise)
    const releaseError = new Error('release failed')
    const { Queue, abortController, complete, fail, errorSpy } = await startWorker({
      fetch: vi.fn().mockResolvedValue([createJob('job-1'), createJob('job-2')]),
      handle,
      executeSql: vi.fn().mockRejectedValue(releaseError),
    })

    await vi.advanceTimersByTimeAsync(1_000)
    abortController.abort()
    const stopping = Queue.stop()
    running.resolve()
    await stopping

    expect(handle).toHaveBeenCalledOnce()
    expect(complete).toHaveBeenCalledExactlyOnceWith('test-queue', 'job-1')
    expect(fail).not.toHaveBeenCalled()
    expect(errorSpy).toHaveBeenCalledWith(
      expect.anything(),
      '[Queue] Error releasing jobs waiting during shutdown TestEvent',
      expect.objectContaining({ error: releaseError })
    )
  })

  it('restores the fetch budget after failed jobs', async () => {
    vi.useFakeTimers()

    const { abortController, fail, fetch } = await startWorker({
      fetch: vi
        .fn()
        .mockResolvedValueOnce([createJob('job-1'), createJob('job-2')])
        .mockResolvedValue([]),
      handle: vi.fn().mockRejectedValue(new Error('handler failed')),
    })

    await vi.advanceTimersByTimeAsync(1_000)

    expect(fetch).toHaveBeenNthCalledWith(
      1,
      'test-queue',
      expect.objectContaining({ batchSize: 2 })
    )
    expect(fail).toHaveBeenCalledTimes(2)

    await vi.advanceTimersByTimeAsync(1_000)

    expect(fetch).toHaveBeenNthCalledWith(
      2,
      'test-queue',
      expect.objectContaining({ batchSize: 2 })
    )

    abortController.abort()
  })

  it('does not fail a succeeded job when completion fails', async () => {
    vi.useFakeTimers()

    const completeError = new Error('complete failed')
    const { monitoringModule, abortController, errorSpy, complete, fail } = await startWorker({
      complete: vi.fn().mockRejectedValue(completeError),
      handle: vi.fn().mockResolvedValue(undefined),
    })

    await vi.advanceTimersByTimeAsync(1_000)
    abortController.abort()

    expect(complete).toHaveBeenCalledTimes(1)
    expect(fail).not.toHaveBeenCalled()
    expect(errorSpy).toHaveBeenCalledWith(
      monitoringModule.logger,
      '[Queue Handler] Error while completing job TestEvent',
      expect.objectContaining({
        type: 'queue-task',
        error: completeError,
      })
    )
  })

  it('logs the original handler error when marking the job as failed rejects', async () => {
    vi.useFakeTimers()

    const handlerError = new Error('handler failed')
    const failError = new Error('fail failed')
    const { monitoringModule, abortController, errorSpy, complete, fail } = await startWorker({
      fail: vi.fn().mockRejectedValue(failError),
      handle: vi.fn().mockRejectedValue(handlerError),
    })

    await vi.advanceTimersByTimeAsync(1_000)
    abortController.abort()

    expect(fail).toHaveBeenCalledTimes(1)
    expect(complete).not.toHaveBeenCalled()
    expect(errorSpy).toHaveBeenCalledWith(
      monitoringModule.logger,
      '[Queue Handler] Error while processing job TestEvent',
      expect.objectContaining({
        type: 'queue-task',
        error: handlerError,
      })
    )
    expect(errorSpy).toHaveBeenCalledWith(
      monitoringModule.logger,
      '[Queue Handler] Error while marking job as failed TestEvent',
      expect.objectContaining({
        type: 'queue-task',
        error: failError,
      })
    )
  })

  it('drains in-flight jobs before stopping pg-boss and running close hooks', async () => {
    vi.useFakeTimers()
    const job = Promise.withResolvers<void>()
    const { Queue, abortController, stop } = await startWorker({ handle: vi.fn(() => job.promise) })
    const onClose = vi.fn()
    ;(Queue as unknown as { events: unknown[] }).events = [{ onClose }]

    await vi.advanceTimersByTimeAsync(1_000)
    abortController.abort()
    const stopping = Queue.stop()
    await vi.advanceTimersByTimeAsync(0)
    expect(stop).not.toHaveBeenCalled()
    expect(onClose).not.toHaveBeenCalled()

    job.resolve()
    await stopping
    expect(stop).toHaveBeenCalledOnce()
    expect(onClose).toHaveBeenCalledOnce()
  })

  it.each([
    ['its own stop budget', undefined, 25_001],
    ['an earlier shutdown deadline', 5_000, 5_001],
  ])('gives up on unfinished jobs within %s', async (_label, deadlineIn, elapsed) => {
    vi.useFakeTimers()
    const { Queue, abortController, errorSpy } = await startWorker({
      handle: vi.fn(() => new Promise<void>(() => undefined)),
    })
    const stop = vi.fn(() => new Promise<void>(() => undefined))
    const onClose = vi.fn()
    ;(Queue as unknown as { pgBoss: { stop: unknown } }).pgBoss.stop = stop
    ;(Queue as unknown as { events: unknown[] }).events = [{ onClose }]

    await vi.advanceTimersByTimeAsync(1_000)
    abortController.abort()
    const deadline = deadlineIn ? Date.now() + deadlineIn : undefined
    const failure = Queue.stop(deadline).catch((error) => error)
    await vi.advanceTimersByTimeAsync(elapsed)

    expect(await failure).toEqual(
      expect.objectContaining({ message: 'Queue stop timed out after 5000ms' })
    )
    expect(stop).toHaveBeenCalledOnce()
    expect(onClose).toHaveBeenCalledOnce()
    expect(errorSpy).toHaveBeenCalledWith(
      expect.anything(),
      '[Queue] Error while draining in-flight jobs',
      expect.objectContaining({ type: 'queue' })
    )
  })

  it('waits for an outstanding fetch and job release before pg-boss stops', async () => {
    vi.useFakeTimers()
    const fetched = Promise.withResolvers<unknown[]>()
    const released = Promise.withResolvers<{ rows: [] }>()
    const { Queue, abortController, stop, fail, executeSql } = await startWorker({
      fetch: vi.fn(() => fetched.promise),
      handle: vi.fn(),
      executeSql: vi.fn(() => released.promise),
    })
    const onClose = vi.fn()
    ;(Queue as unknown as { events: unknown[] }).events = [{ onClose }]

    await vi.advanceTimersByTimeAsync(1_000)
    abortController.abort()
    const stopping = Queue.stop()
    await vi.advanceTimersByTimeAsync(0)
    expect(stop).not.toHaveBeenCalled()

    fetched.resolve([createJob('job-1')])
    await vi.advanceTimersByTimeAsync(0)
    expect(executeSql).toHaveBeenCalledWith(expect.any(String), ['test-queue', ['job-1']])
    expect(stop).not.toHaveBeenCalled()

    released.resolve({ rows: [] })
    await stopping
    expect(fail).not.toHaveBeenCalled()
    expect(stop).toHaveBeenCalledOnce()
    expect(onClose).toHaveBeenCalledOnce()
  })
})

describe('Queue.stop', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.resetModules()
  })

  it('times out a hung pg-boss stop and clears cached queue state', async () => {
    vi.useFakeTimers()

    const {
      queueModule: { Queue },
    } = await loadQueueModule()
    const boss = {
      stop: vi.fn(() => new Promise<void>(() => undefined)),
    }
    const db = {}

    ;(Queue as unknown as { pgBoss: unknown }).pgBoss = boss
    ;(Queue as unknown as { pgBossDb: unknown }).pgBossDb = db

    const stopPromise = Queue.stop()
    const stopErrorPromise = stopPromise.catch((error) => error)

    await vi.advanceTimersByTimeAsync(25_000)

    expect(await stopErrorPromise).toEqual(
      expect.objectContaining({
        message: 'Queue stop timed out after 25000ms',
      })
    )
    expect(boss.stop).toHaveBeenCalledWith({
      timeout: 20 * 1000,
      graceful: false,
      wait: true,
    })
    expect((Queue as unknown as { pgBoss?: unknown }).pgBoss).toBeUndefined()
    expect((Queue as unknown as { pgBossDb?: unknown }).pgBossDb).toBeUndefined()
  })

  it('clears cached queue state when close hooks time out', async () => {
    vi.useFakeTimers()

    const {
      queueModule: { Queue },
    } = await loadQueueModule()
    const boss = {
      stop: vi.fn().mockResolvedValue(undefined),
    }
    const db = {}

    ;(Queue as unknown as { pgBoss: unknown }).pgBoss = boss
    ;(Queue as unknown as { pgBossDb: unknown }).pgBossDb = db
    ;(Queue as unknown as { events: Array<{ onClose: () => Promise<void> }> }).events = [
      {
        onClose: () => new Promise<void>(() => undefined),
      },
    ]

    const stopPromise = Queue.stop()
    const stopErrorPromise = stopPromise.catch((error) => error)

    await vi.advanceTimersByTimeAsync(25_000)

    expect(await stopErrorPromise).toEqual(
      expect.objectContaining({
        message: 'Queue close timed out after 25000ms',
      })
    )
    expect((Queue as unknown as { pgBoss?: unknown }).pgBoss).toBeUndefined()
    expect((Queue as unknown as { pgBossDb?: unknown }).pgBossDb).toBeUndefined()
  })
})
