import { ERRORS } from '@internal/errors'
import {
  queueJobCompleted,
  queueJobRetryFailed,
  queueJobRunTime,
  queueJobScheduled,
  queueJobSchedulingTime,
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
}))

import { getTenantConfig } from '@internal/database'
import { getConfig, mergeConfig } from '../../config'
import {
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

const consumeCall = { queue: 'test-event' } as unknown as ConsumeCall

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

  it('rethrows a failed batch append without dispatching any message', async () => {
    const { call, invoke } = makeSyncCall([{ data: tenantData }, { data: tenantData }])
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
