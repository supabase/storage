import { queueJobScheduled, queueJobSchedulingTime } from '@internal/monitoring/metrics'
import type { ProduceCall } from '@supabase-labs/wave-core'
import { vi } from 'vitest'

vi.mock('@internal/database', () => ({
  getTenantConfig: vi.fn(),
}))

import { schedulingMetrics } from './middleware'

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

describe('schedulingMetrics', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('records queue scheduling duration from numeric monotonic timestamps', async () => {
    const recordSpy = vi.spyOn(queueJobSchedulingTime, 'record').mockImplementation(() => {})
    const addSpy = vi.spyOn(queueJobScheduled, 'add').mockImplementation(() => {})
    vi.spyOn(performance, 'now').mockReturnValueOnce(20).mockReturnValueOnce(26)
    const next = vi.fn().mockResolvedValue(undefined)

    await produceStage()(next)(makeCall(2))

    expect(recordSpy).toHaveBeenCalledWith(0.006, { name: 'test-event' })
    expect(addSpy).toHaveBeenCalledWith(2, { name: 'test-event' })
  })

  it('still records the duration when the produce fails, but counts nothing scheduled', async () => {
    const recordSpy = vi.spyOn(queueJobSchedulingTime, 'record').mockImplementation(() => {})
    const addSpy = vi.spyOn(queueJobScheduled, 'add').mockImplementation(() => {})
    vi.spyOn(performance, 'now').mockReturnValueOnce(20).mockReturnValueOnce(26)
    const next = vi.fn().mockRejectedValue(new Error('append failed'))

    await expect(produceStage()(next)(makeCall())).rejects.toThrow('append failed')

    expect(recordSpy).toHaveBeenCalledWith(0.006, { name: 'test-event' })
    expect(addSpy).not.toHaveBeenCalled()
  })
})
