import { ERRORS } from '@internal/errors'
import {
  queueJobCompleted,
  queueJobRetryFailed,
  queueJobRunTime,
  queueJobScheduled,
  queueJobSchedulingTime,
  queueJobWaitTime,
} from '@internal/monitoring/metrics'
import { vi } from 'vitest'
import type { BatchResult, ConsumeCall, Message, ProduceCall } from './manager'

vi.mock('@internal/database', () => ({
  getTenantConfig: vi.fn(),
}))

// Without an OTel SDK registered, every no-op counter is ONE shared singleton — spying on
// `queueJobCompleted.add` would also observe `queueJobScheduled.add`. Distinct plain objects
// keep the per-metric assertions honest.
vi.mock('@internal/monitoring/metrics', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  queueJobScheduled: { add: vi.fn() },
  queueJobSchedulingTime: { record: vi.fn() },
  queueJobCompleted: { add: vi.fn() },
  queueJobRetryFailed: { add: vi.fn() },
  queueJobRunTime: { record: vi.fn() },
  queueJobWaitTime: { record: vi.fn() },
}))

import { getTenantConfig } from '@internal/database'
import { getConfig, mergeConfig } from '../../config'
import {
  completionLog,
  isSyncDispatch,
  SYNC_DISPATCH_HEADER,
  schedulingMetrics,
  syncFallback,
  syncModeGuard,
  tenantDisableEvents,
} from './middleware'

function makeCall(messageCount = 1): ProduceCall {
  return {
    queue: 'test-event',
    messages: Array.from({ length: messageCount }, () => ({ data: {} })),
  } as unknown as ProduceCall
}

// `QueueMiddleware` is a union (bare handler middleware | {handle?, produce?}); narrow to the
// produce stage schedulingMetrics always declares.
function produceStage() {
  const mw = schedulingMetrics()
  if (typeof mw === 'function' || mw.produce === undefined) {
    throw new Error('schedulingMetrics is expected to declare a produce middleware')
  }
  return mw.produce
}

function consumeStage() {
  const mw = schedulingMetrics()
  if (typeof mw === 'function' || mw.consume === undefined) {
    throw new Error('schedulingMetrics is expected to declare a consume middleware')
  }
  return mw.consume
}

const consumeCall = { queue: 'test-event', messages: [] } as unknown as ConsumeCall

/** A `BatchResult` with `ok` successes and one failure per listed delivery attempt. */
function batchResult(ok: number, failedAttempts: number[] = []): BatchResult {
  const failures = failedAttempts.map((attempt) => ({
    message: { attempt } as unknown as Message<unknown>,
    error: new Error('handler failed'),
  }))
  return {
    outcome: failures.length > 0 ? 'error' : 'ok',
    size: ok + failures.length,
    ok,
    released: 0,
    failures,
  }
}

describe('tenantDisableEvents', () => {
  function gateStage() {
    const mw = tenantDisableEvents()
    if (typeof mw === 'function' || mw.produce === undefined) {
      throw new Error('tenantDisableEvents is expected to declare a produce middleware')
    }
    return mw.produce
  }

  function makeTenantCall(refs: string[]): ProduceCall {
    return {
      queue: 'test-event',
      messages: refs.map((ref) => ({
        data: { tenant: { ref, host: `${ref}.local.test` } },
        headers: {},
      })),
      getQueue: () => ({ classes: [{ eventType: 'test-event' }] }),
    } as unknown as ProduceCall
  }

  beforeEach(() => {
    getConfig({ reload: true })
    mergeConfig({ isMultitenant: true })
  })

  afterEach(() => {
    vi.clearAllMocks()
    getConfig({ reload: true })
  })

  it('drops messages for tenants that no longer exist instead of failing the produce', async () => {
    vi.mocked(getTenantConfig).mockImplementation(async (ref) => {
      if (ref === 'tenant-gone') throw ERRORS.MissingTenantConfig(ref)
      return { disableEvents: [] } as never
    })
    const next = vi.fn().mockResolvedValue(undefined)

    await gateStage()(next)(makeTenantCall(['tenant-gone', 'tenant-a']))

    expect(next).toHaveBeenCalledTimes(1)
    const forwarded = next.mock.calls[0][0] as ProduceCall
    expect(forwarded.messages).toHaveLength(1)
    expect((forwarded.messages[0].data as { tenant: { ref: string } }).tenant.ref).toBe('tenant-a')
  })

  it('resolves without producing when every message belongs to a missing tenant', async () => {
    vi.mocked(getTenantConfig).mockRejectedValue(ERRORS.MissingTenantConfig('tenant-gone'))
    const next = vi.fn().mockResolvedValue(undefined)

    await expect(gateStage()(next)(makeTenantCall(['tenant-gone']))).resolves.toBeUndefined()

    expect(next).not.toHaveBeenCalled()
  })

  it("looks each distinct tenant up once and drops only the disabling tenant's messages", async () => {
    vi.mocked(getTenantConfig).mockImplementation(
      async (ref) => ({ disableEvents: ref === 'tenant-b' ? ['test-event'] : [] }) as never
    )
    const next = vi.fn().mockResolvedValue(undefined)

    await gateStage()(next)(makeTenantCall(['tenant-a', 'tenant-b', 'tenant-a', 'tenant-b']))

    expect(getTenantConfig).toHaveBeenCalledTimes(2)
    const forwarded = next.mock.calls[0][0] as ProduceCall
    expect(
      forwarded.messages.map((m) => (m.data as { tenant: { ref: string } }).tenant.ref)
    ).toEqual(['tenant-a', 'tenant-a'])
  })

  it('looks tenants up concurrently', async () => {
    let inFlight = 0
    let peak = 0
    vi.mocked(getTenantConfig).mockImplementation(async () => {
      peak = Math.max(peak, ++inFlight)
      await new Promise((resolve) => setImmediate(resolve))
      inFlight--
      return { disableEvents: [] } as never
    })
    const refs = Array.from({ length: 50 }, (_, i) => `tenant-${i}`)

    await gateStage()(vi.fn().mockResolvedValue(undefined))(makeTenantCall(refs))

    expect(getTenantConfig).toHaveBeenCalledTimes(50)
    expect(peak).toBeGreaterThan(1)
    expect(peak).toBeLessThanOrEqual(20)
  })

  it('loads no tenant config for an event that declares no disable keys', async () => {
    const next = vi.fn().mockResolvedValue(undefined)
    const call = {
      ...makeTenantCall(['tenant-a', 'tenant-b']),
      getQueue: () => ({ classes: [{ eventType: 'test-event', disableKeys: () => [] }] }),
    } as unknown as ProduceCall

    await gateStage()(next)(call)

    expect(getTenantConfig).not.toHaveBeenCalled()
    expect(next).toHaveBeenCalledWith(call)
  })

  it('still propagates non-TenantNotFound errors from the tenant config lookup', async () => {
    vi.mocked(getTenantConfig).mockRejectedValue(new Error('db unreachable'))
    const next = vi.fn().mockResolvedValue(undefined)

    await expect(gateStage()(next)(makeTenantCall(['tenant-a']))).rejects.toThrow('db unreachable')

    expect(next).not.toHaveBeenCalled()
  })
})

describe('schedulingMetrics', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    // The metric mocks are module-level vi.fn()s — restore alone never clears their history.
    vi.clearAllMocks()
  })

  it('records queue scheduling duration from numeric monotonic timestamps', async () => {
    const recordSpy = vi.spyOn(queueJobSchedulingTime, 'record').mockImplementation(() => {})
    const addSpy = vi.spyOn(queueJobScheduled, 'add').mockImplementation(() => {})
    vi.spyOn(performance, 'now').mockReturnValueOnce(20).mockReturnValueOnce(26)
    const next = vi.fn().mockResolvedValue(undefined)

    await produceStage()(next)(makeCall(2))

    expect(recordSpy).toHaveBeenCalledWith(0.006, { name: 'test-event', adapter: 'pgboss' })
    expect(addSpy).toHaveBeenCalledWith(2, { name: 'test-event', adapter: 'pgboss' })
  })

  it('still records the duration when the produce fails, but counts nothing scheduled', async () => {
    const recordSpy = vi.spyOn(queueJobSchedulingTime, 'record').mockImplementation(() => {})
    const addSpy = vi.spyOn(queueJobScheduled, 'add').mockImplementation(() => {})
    vi.spyOn(performance, 'now').mockReturnValueOnce(20).mockReturnValueOnce(26)
    const next = vi.fn().mockRejectedValue(new Error('append failed'))

    await expect(produceStage()(next)(makeCall())).rejects.toThrow('append failed')

    expect(recordSpy).toHaveBeenCalledWith(0.006, { name: 'test-event', adapter: 'pgboss' })
    expect(addSpy).not.toHaveBeenCalled()
  })

  describe('consume', () => {
    it('records how long each first delivery waited, and skips redeliveries and in-process runs', async () => {
      vi.useFakeTimers({ now: 1_000_000 })
      try {
        const message = (fields: Partial<Message<unknown>>) =>
          ({ attempt: 1, timestamp: 1_000_000, headers: {}, ...fields }) as Message<unknown>
        const call = {
          queue: 'test-event',
          messages: [
            message({ timestamp: 1_000_000 - 2_500 }),
            // The backend's clock is slightly ahead of this process: never negative.
            message({ timestamp: 1_000_000 + 40 }),
            message({ attempt: 2, timestamp: 1_000_000 - 60_000 }),
            message({ timestamp: 1_000_000 - 9_000, headers: { [SYNC_DISPATCH_HEADER]: '1' } }),
          ],
        } as unknown as ConsumeCall

        await consumeStage()(vi.fn().mockResolvedValue(batchResult(4)))(call)

        const labels = { name: 'test-event', adapter: getConfig().queueAdapter }
        expect(vi.mocked(queueJobWaitTime.record).mock.calls).toEqual([
          [2.5, labels],
          [0, labels],
        ])
      } finally {
        vi.useRealTimers()
      }
    })

    it('counts handled messages into queue_job_completed and passes the result through', async () => {
      const result = batchResult(3)
      const next = vi.fn().mockResolvedValue(result)

      await expect(consumeStage()(next)(consumeCall)).resolves.toBe(result)

      expect(queueJobCompleted.add).toHaveBeenCalledWith(3, {
        name: 'test-event',
        adapter: 'pgboss',
      })
      expect(queueJobRetryFailed.add).not.toHaveBeenCalled()
    })

    it('counts every handler failure into queue_job_retry_failed and nothing into completed', async () => {
      const next = vi.fn().mockResolvedValue(batchResult(0, [2, 1]))

      await consumeStage()(next)(consumeCall)

      expect(queueJobCompleted.add).not.toHaveBeenCalled()
      expect(queueJobRetryFailed.add).toHaveBeenCalledWith(2, {
        name: 'test-event',
        adapter: 'pgboss',
      })
      expect(queueJobRunTime.record).toHaveBeenCalledWith(expect.any(Number), {
        name: 'test-event',
        adapter: 'pgboss',
        status: 'error',
      })
    })

    it('times the invocation and labels the duration with the batch outcome', async () => {
      vi.spyOn(performance, 'now').mockReturnValueOnce(20).mockReturnValueOnce(26)
      const next = vi.fn().mockResolvedValue(batchResult(1))

      await consumeStage()(next)(consumeCall)

      expect(queueJobRunTime.record).toHaveBeenCalledWith(0.006, {
        name: 'test-event',
        adapter: 'pgboss',
        status: 'ok',
      })
    })

    it('records both sides of a partially failed batch', async () => {
      const next = vi.fn().mockResolvedValue(batchResult(2, [1]))

      await consumeStage()(next)(consumeCall)

      expect(queueJobCompleted.add).toHaveBeenCalledWith(2, {
        name: 'test-event',
        adapter: 'pgboss',
      })
      expect(queueJobRetryFailed.add).toHaveBeenCalledWith(1, {
        name: 'test-event',
        adapter: 'pgboss',
      })
    })
  })
})

describe('isSyncDispatch', () => {
  it('is true only for a message stamped with the sync-dispatch header', () => {
    expect(SYNC_DISPATCH_HEADER).toBe('x-sync-dispatch')
    expect(isSyncDispatch({ headers: { [SYNC_DISPATCH_HEADER]: '1' } })).toBe(true)
    expect(isSyncDispatch({ headers: { 'x-sync-dispatch': '1', other: 'kept' } })).toBe(true)
  })

  it('is false for a message delivered from the queue', () => {
    expect(isSyncDispatch({})).toBe(false)
    expect(isSyncDispatch({ headers: {} })).toBe(false)
    expect(isSyncDispatch({ headers: { other: '1' } })).toBe(false)
    // Only the exact marker value counts.
    expect(isSyncDispatch({ headers: { [SYNC_DISPATCH_HEADER]: '0' } })).toBe(false)
    expect(isSyncDispatch({ headers: { [SYNC_DISPATCH_HEADER]: 'true' } })).toBe(false)
  })
})

type TestMessage = { data: unknown; key?: string; headers?: Record<string, string> }

/** A produce call on a single-class queue whose bound class declares `allowSync`. */
function makeSyncCall(messages: TestMessage[], allowSync?: boolean) {
  const invoke = vi.fn().mockResolvedValue(undefined)
  const call = {
    queue: 'test-event',
    messages,
    invoke,
    getQueue: () => ({
      classes: [{ eventType: 'test-event', ...(allowSync === undefined ? {} : { allowSync }) }],
    }),
  } as unknown as ProduceCall
  return { call, invoke }
}

const tenantData = { tenant: { ref: 'tenant-a', host: 'tenant-a.local.test' } }

describe('syncFallback', () => {
  function fallbackStage() {
    const mw = syncFallback()
    if (typeof mw === 'function' || mw.produce === undefined) {
      throw new Error('syncFallback is expected to declare a produce middleware')
    }
    return mw.produce
  }

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('does not dispatch inline when the append succeeds', async () => {
    const { call, invoke } = makeSyncCall([{ data: tenantData }])
    const next = vi.fn().mockResolvedValue(undefined)

    await expect(fallbackStage()(next)(call)).resolves.toBeUndefined()

    expect(next).toHaveBeenCalledExactlyOnceWith(call)
    expect(invoke).not.toHaveBeenCalled()
  })

  it('stamps the sync-dispatch header on the invoke it falls back to', async () => {
    const { call, invoke } = makeSyncCall([
      { data: tenantData, key: 'object-key', headers: { 'message-type': 'test-event' } },
    ])
    const next = vi.fn().mockRejectedValue(new Error('append failed'))

    await expect(fallbackStage()(next)(call)).resolves.toBeUndefined()

    expect(invoke).toHaveBeenCalledExactlyOnceWith('test-event', tenantData, {
      key: 'object-key',
      headers: { 'message-type': 'test-event', [SYNC_DISPATCH_HEADER]: '1' },
    })
    expect(isSyncDispatch(invoke.mock.calls[0][2])).toBe(true)
    // The produced message itself is left untouched: only the inline run is marked.
    expect(call.messages[0].headers).toEqual({ 'message-type': 'test-event' })
  })

  it('stamps the header on a fallback for a message that carried no headers', async () => {
    const { call, invoke } = makeSyncCall([{ data: tenantData }])
    const next = vi.fn().mockRejectedValue(new Error('append failed'))

    await fallbackStage()(next)(call)

    expect(invoke).toHaveBeenCalledExactlyOnceWith('test-event', tenantData, {
      key: undefined,
      headers: { [SYNC_DISPATCH_HEADER]: '1' },
    })
  })

  it('surfaces the inline handler failure of a fallback', async () => {
    const { call, invoke } = makeSyncCall([{ data: tenantData }])
    const handlerError = new Error('handler failed')
    invoke.mockRejectedValue(handlerError)
    const next = vi.fn().mockRejectedValue(new Error('append failed'))

    await expect(fallbackStage()(next)(call)).rejects.toBe(handlerError)
  })

  it('rethrows the append failure without dispatching an allowSync: false event', async () => {
    const { call, invoke } = makeSyncCall([{ data: tenantData }], false)
    const appendError = new Error('append failed')
    const next = vi.fn().mockRejectedValue(appendError)

    await expect(fallbackStage()(next)(call)).rejects.toBe(appendError)

    expect(invoke).not.toHaveBeenCalled()
  })

  it('runs every message of a failed batch in-process', async () => {
    const other = { tenant: { ref: 'tenant-b', host: 'tenant-b.local.test' } }
    const { call, invoke } = makeSyncCall([{ data: tenantData, key: 'k1' }, { data: other }])
    const next = vi.fn().mockRejectedValue(new Error('append failed'))

    await expect(fallbackStage()(next)(call)).resolves.toBeUndefined()

    expect(invoke.mock.calls).toEqual([
      ['test-event', tenantData, { key: 'k1', headers: { [SYNC_DISPATCH_HEADER]: '1' } }],
      ['test-event', other, { key: undefined, headers: { [SYNC_DISPATCH_HEADER]: '1' } }],
    ])
  })

  it('runs a large failed batch a bounded number of handlers at a time', async () => {
    const messages = Array.from({ length: 45 }, () => ({ data: tenantData }))
    const { call, invoke } = makeSyncCall(messages)
    let running = 0
    let peak = 0
    invoke.mockImplementation(async () => {
      peak = Math.max(peak, ++running)
      await new Promise((resolve) => setImmediate(resolve))
      running--
    })
    const next = vi.fn().mockRejectedValue(new Error('append failed'))

    await fallbackStage()(next)(call)

    expect(invoke).toHaveBeenCalledTimes(45)
    expect(peak).toBe(20)
  })

  it('gives every message of a failed batch its turn, then surfaces the first handler failure', async () => {
    const { call, invoke } = makeSyncCall([
      { data: tenantData },
      { data: tenantData },
      { data: tenantData },
    ])
    const handlerError = new Error('handler failed')
    invoke.mockRejectedValueOnce(handlerError)
    const next = vi.fn().mockRejectedValue(new Error('append failed'))

    await expect(fallbackStage()(next)(call)).rejects.toBe(handlerError)

    expect(invoke).toHaveBeenCalledTimes(3)
  })

  it('rethrows a failed batch append of allowSync: false events without dispatching', async () => {
    const { call, invoke } = makeSyncCall([{ data: tenantData }, { data: tenantData }], false)
    const appendError = new Error('append failed')
    const next = vi.fn().mockRejectedValue(appendError)

    await expect(fallbackStage()(next)(call)).rejects.toBe(appendError)

    expect(invoke).not.toHaveBeenCalled()
  })
})

describe('syncModeGuard', () => {
  function guardStage() {
    const mw = syncModeGuard()
    if (typeof mw === 'function' || mw.produce === undefined) {
      throw new Error('syncModeGuard is expected to declare a produce middleware')
    }
    return mw.produce
  }

  it('stamps the sync-dispatch header on every message it forwards', async () => {
    const { call } = makeSyncCall([
      { data: { n: 1 } },
      { data: { n: 2 }, key: 'k', headers: { 'message-type': 'test-event' } },
    ])
    const next = vi.fn().mockResolvedValue(undefined)

    await guardStage()(next)(call)

    expect(next).toHaveBeenCalledTimes(1)
    const forwarded = next.mock.calls[0][0] as ProduceCall
    expect(forwarded.queue).toBe('test-event')
    expect(forwarded.messages).toEqual([
      { data: { n: 1 }, headers: { [SYNC_DISPATCH_HEADER]: '1' } },
      {
        data: { n: 2 },
        key: 'k',
        headers: { 'message-type': 'test-event', [SYNC_DISPATCH_HEADER]: '1' },
      },
    ])
    expect(forwarded.messages.every(isSyncDispatch)).toBe(true)
    // The caller's messages are not mutated.
    expect(call.messages[0]).toEqual({ data: { n: 1 } })
  })

  it('stamps messages on a topic with no storage event class bound', async () => {
    const call = {
      queue: 'plain-topic',
      messages: [{ data: {} }],
      getQueue: () => undefined,
    } as unknown as ProduceCall
    const next = vi.fn().mockResolvedValue(undefined)

    await guardStage()(next)(call)

    expect((next.mock.calls[0][0] as ProduceCall).messages.every(isSyncDispatch)).toBe(true)
  })

  it('skips allowSync: false events instead of running them inline', async () => {
    const { call } = makeSyncCall([{ data: tenantData }], false)
    const next = vi.fn().mockResolvedValue(undefined)

    await expect(guardStage()(next)(call)).resolves.toBeUndefined()

    expect(next).not.toHaveBeenCalled()
  })
})

describe('completionLog', () => {
  async function run(
    messages: Message<unknown>[],
    result: Partial<BatchResult> = {},
    queue = 'webhooks'
  ) {
    const { logSchema } = await import('@internal/monitoring')
    const event = vi.spyOn(logSchema, 'event').mockImplementation(() => undefined)
    const mw = completionLog()
    if (mw.consume === undefined) throw new Error('completionLog declares a consume middleware')
    const outcome: BatchResult = {
      outcome: 'ok',
      size: messages.length,
      ok: messages.length,
      released: 0,
      failures: [],
      ...result,
    }
    const returned = await mw.consume(async () => outcome)({
      queue,
      messages,
    } as unknown as ConsumeCall)
    expect(returned).toBe(outcome)
    return event
  }
  const message = (data: unknown, extra: Partial<Message<unknown>> = {}): Message<unknown> => ({
    id: 'job-1',
    data,
    headers: {},
    timestamp: 0,
    attempt: 1,
    ...extra,
  })
  const tenant = { ref: 'tenant-a', host: 'tenant-a.local' }

  it('logs a completed message under its message type, with its payload and ids', async () => {
    const data = { tenant, bucketId: 'bucket', name: 'a/b.png', reqId: 'req-1', sbReqId: 'sb-1' }

    const event = await run(
      [message(data, { type: 'ObjectAdminDelete' })],
      {},
      'object-admin-delete'
    )

    expect(event).toHaveBeenCalledExactlyOnceWith(
      expect.anything(),
      '[ObjectAdminDelete]: tenant-a/bucket/a/b.png - COMPLETED',
      {
        jobId: 'job-1',
        type: 'event',
        event: 'ObjectAdminDelete',
        payload: JSON.stringify(data),
        objectPath: 'tenant-a/bucket/a/b.png',
        resources: ['/tenant-a/bucket/a/b.png'],
        tenantId: 'tenant-a',
        project: 'tenant-a',
        reqId: 'req-1',
        sbReqId: 'sb-1',
        attempt: 1,
        durationMs: expect.any(Number),
      }
    )
  })

  it('names the bucket and object of a CDN purge', async () => {
    const data = {
      tenant,
      purgeOptions: { type: 'object', tenant: 'tenant-a', bucket: 'b', objectName: 'o.png' },
    }

    const event = await run([message(data, { type: 'PurgeCdnCache' })], {}, 'cdn-purge-cache')

    expect(event.mock.calls[0][1]).toBe('[PurgeCdnCache]: tenant-a/b/o.png - COMPLETED')
  })

  it('names the wrapped event and its object for a webhook', async () => {
    const data = {
      tenant,
      event: { type: 'ObjectCreated:Put', payload: { bucketId: 'b', name: 'o' } },
    }

    const event = await run([message(data, { type: 'Webhook' })])

    expect(event.mock.calls[0][1]).toBe('[Webhook]: ObjectCreated:Put tenant-a/b/o - COMPLETED')
  })

  it('falls back to the queue name for a message sent without a class', async () => {
    const event = await run([message({ n: 1 })], {}, 'plain-queue')

    expect(event.mock.calls[0][1]).toBe('[plain-queue]: SYSTEM_TENANT - COMPLETED')
  })

  it('logs a failed message as FAILED, with its error, attempt and duration', async () => {
    const { logger } = await import('@internal/monitoring')
    const logError = vi.spyOn(logger, 'error').mockImplementation(() => undefined)
    const ok = message({ tenant }, { id: 'ok', type: 'T' })
    const failed = message(
      { tenant, bucketId: 'b', name: 'o' },
      { id: 'failed', type: 'T', attempt: 3 }
    )
    const boom = new Error('boom')

    const event = await run([ok, failed], {
      outcome: 'error',
      ok: 1,
      failures: [{ message: failed, error: boom }],
    })

    expect(event).toHaveBeenCalledTimes(1)
    expect(event.mock.calls[0][2]).toMatchObject({ jobId: 'ok' })
    expect(logError).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        jobId: 'failed',
        error: boom,
        attempt: 3,
        durationMs: expect.any(Number),
        objectPath: 'tenant-a/b/o',
      }),
      '[T]: tenant-a/b/o - FAILED'
    )
  })

  it('logs nothing for a timed-out or released invocation', async () => {
    for (const outcome of ['timeout', 'shutdown'] as const) {
      const event = await run([message({ tenant })], { outcome, ok: 0, released: 1 })
      expect(event).not.toHaveBeenCalled()
      event.mockRestore()
    }
  })

  it('never fails the invocation when the payload cannot be logged', async () => {
    const cyclic: Record<string, unknown> = { tenant }
    cyclic.self = cyclic

    const event = await run([message(cyclic)])

    expect(event).not.toHaveBeenCalled()
  })
})
