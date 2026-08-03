import { ERRORS } from '@internal/errors'
import {
  queueJobCompleted,
  queueJobRetryFailed,
  queueJobRunTime,
  queueJobScheduled,
  queueJobSchedulingTime,
} from '@internal/monitoring/metrics'
import type { BatchResult, ConsumeCall, Envelope, ProduceCall } from '@supabase-labs/wave-core'
import { vi } from 'vitest'

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
import { schedulingMetrics, tenantDisableEvents } from './middleware'

function makeCall(messageCount = 1): ProduceCall {
  return {
    topic: 'test-event',
    messages: Array.from({ length: messageCount }, () => ({ data: {} })),
  } as unknown as ProduceCall
}

// `WaveMiddleware` is a union (bare handler middleware | {handle?, produce?}); narrow to the
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

const consumeCall = { topic: 'test-event' } as unknown as ConsumeCall

/** A `BatchResult` with `ok` successes and one failure per listed delivery attempt. */
function batchResult(ok: number, failedAttempts: number[] = []): BatchResult {
  const failures = failedAttempts.map((attempt) => ({
    message: { attempt } as unknown as Envelope<unknown>,
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
      topic: 'test-event',
      messages: refs.map((ref) => ({
        data: { tenant: { ref, host: `${ref}.local.test` } },
        headers: {},
      })),
      getTopic: () => ({ classes: [{ eventType: 'test-event' }] }),
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

    expect(recordSpy).toHaveBeenCalledWith(0.006, { name: 'test-event', adapter: 'pgque' })
    expect(addSpy).toHaveBeenCalledWith(2, { name: 'test-event', adapter: 'pgque' })
  })

  it('still records the duration when the produce fails, but counts nothing scheduled', async () => {
    const recordSpy = vi.spyOn(queueJobSchedulingTime, 'record').mockImplementation(() => {})
    const addSpy = vi.spyOn(queueJobScheduled, 'add').mockImplementation(() => {})
    vi.spyOn(performance, 'now').mockReturnValueOnce(20).mockReturnValueOnce(26)
    const next = vi.fn().mockRejectedValue(new Error('append failed'))

    await expect(produceStage()(next)(makeCall())).rejects.toThrow('append failed')

    expect(recordSpy).toHaveBeenCalledWith(0.006, { name: 'test-event', adapter: 'pgque' })
    expect(addSpy).not.toHaveBeenCalled()
  })

  describe('consume', () => {
    it('counts handled messages into queue_job_completed and passes the result through', async () => {
      const result = batchResult(3)
      const next = vi.fn().mockResolvedValue(result)

      await expect(consumeStage()(next)(consumeCall)).resolves.toBe(result)

      expect(queueJobCompleted.add).toHaveBeenCalledWith(3, {
        name: 'test-event',
        adapter: 'pgque',
      })
      expect(queueJobRetryFailed.add).not.toHaveBeenCalled()
    })

    it('counts every handler failure into queue_job_retry_failed and nothing into completed', async () => {
      const next = vi.fn().mockResolvedValue(batchResult(0, [2, 1]))

      await consumeStage()(next)(consumeCall)

      expect(queueJobCompleted.add).not.toHaveBeenCalled()
      expect(queueJobRetryFailed.add).toHaveBeenCalledWith(2, {
        name: 'test-event',
        adapter: 'pgque',
      })
      expect(queueJobRunTime.record).toHaveBeenCalledWith(expect.any(Number), {
        name: 'test-event',
        adapter: 'pgque',
        status: 'error',
      })
    })

    it('times the invocation and labels the duration with the batch outcome', async () => {
      vi.spyOn(performance, 'now').mockReturnValueOnce(20).mockReturnValueOnce(26)
      const next = vi.fn().mockResolvedValue(batchResult(1))

      await consumeStage()(next)(consumeCall)

      expect(queueJobRunTime.record).toHaveBeenCalledWith(0.006, {
        name: 'test-event',
        adapter: 'pgque',
        status: 'ok',
      })
    })

    it('records both sides of a partially failed batch', async () => {
      const next = vi.fn().mockResolvedValue(batchResult(2, [1]))

      await consumeStage()(next)(consumeCall)

      expect(queueJobCompleted.add).toHaveBeenCalledWith(2, {
        name: 'test-event',
        adapter: 'pgque',
      })
      expect(queueJobRetryFailed.add).toHaveBeenCalledWith(1, {
        name: 'test-event',
        adapter: 'pgque',
      })
    })
  })
})
