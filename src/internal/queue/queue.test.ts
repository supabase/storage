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

describe('Queue worker sbReqId propagation', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
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

    const {
      queueModule: { Queue },
      monitoringModule,
    } = await loadQueueModule()
    const abortController = new AbortController()
    const errorSpy = vi.spyOn(monitoringModule.logSchema, 'error')
    let onMessageSbReqId: string | undefined

    ;(Queue as unknown as { pgBoss: unknown }).pgBoss = {
      complete: vi.fn().mockResolvedValue(undefined),
      fail: vi.fn().mockResolvedValue(undefined),
      fetch: vi
        .fn()
        .mockResolvedValueOnce([
          {
            id: 'job-1',
            data,
            priority: 1,
            retryCount: 0,
            retryLimit: 1,
          },
        ])
        .mockResolvedValueOnce([]),
    }

    ;(Queue as unknown as { pollQueue: Function }).pollQueue(
      {
        getQueueName: () => 'test-queue',
        getWorkerOptions: () => ({
          pollingIntervalSeconds: 1,
        }),
        handle: vi.fn(async () => {
          throw new Error('boom')
        }),
        name: 'TestEvent',
      },
      {
        concurrentTaskCount: 1,
        onMessage: (job: {
          data: {
            sbReqId?: string
            event?: { payload?: { sbReqId?: string } }
          }
        }) => {
          onMessageSbReqId = job.data.sbReqId ?? job.data.event?.payload?.sbReqId
        },
        signal: abortController.signal,
      }
    )

    await vi.advanceTimersByTimeAsync(1_000)
    abortController.abort()

    expect(onMessageSbReqId).toBe(expectedSbReqId)
    const processingErrorCall = errorSpy.mock.calls.find(
      ([, message]) => message === '[Queue Handler] Error while processing job TestEvent'
    )
    expect(processingErrorCall?.[0]).toBe(monitoringModule.logger)
    expect((processingErrorCall?.[2] as { sbReqId?: string }).sbReqId).toBe(expectedSbReqId)
  })
})
