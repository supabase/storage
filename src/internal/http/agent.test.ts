import { afterEach, describe, expect, it, vi } from 'vitest'

vi.mock('@internal/monitoring/metrics', () => ({
  httpPoolBusySockets: { record: vi.fn() },
  httpPoolErrors: { record: vi.fn() },
  httpPoolFreeSockets: { record: vi.fn() },
  httpPoolPendingRequests: { record: vi.fn() },
}))

import { httpPoolBusySockets } from '@internal/monitoring/metrics'
import { createAgent } from './agent'

let agents: ReturnType<typeof createAgent>[] = []

afterEach(async () => {
  for (const agent of agents) await agent.close()
  agents = []
  vi.useRealTimers()
})

describe('createAgent monitor', () => {
  it('restarts sampling after close and keeps the new monitor idempotent', () => {
    vi.useFakeTimers()
    const agent = createAgent('s3_test', { maxSockets: 1 })
    agents.push(agent)
    const first = agent.monitor()
    expect(agent.monitor()).toBe(first)
    expect(vi.getTimerCount()).toBe(1)
    vi.advanceTimersByTime(5_000)
    expect(httpPoolBusySockets.record).toHaveBeenCalledOnce()

    agent.close()
    agent.close()
    const next = agent.monitor()
    expect(next).not.toBe(first)
    expect(agent.monitor()).toBe(next)
    expect(vi.getTimerCount()).toBe(1)
    vi.advanceTimersByTime(5_000)
    expect(httpPoolBusySockets.record).toHaveBeenCalledTimes(2)

    agent.close()
    vi.advanceTimersByTime(5_000)
    expect(httpPoolBusySockets.record).toHaveBeenCalledTimes(2)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('monitor() does not keep the process alive', () => {
    const agent = createAgent('s3_test', { maxSockets: 1 })
    agents.push(agent)
    expect(agent.monitor().hasRef()).toBe(false)
    agent.close()
    expect(agent.monitor().hasRef()).toBe(false)
  })
})
