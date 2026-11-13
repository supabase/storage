import type { QueueSubscription } from './index'
import { pulled } from './test-helpers'
import type { Slot } from './worker/invocation'
import { type Settles, settle, sortSettle } from './worker/settle'

type Op = 'ack' | 'nack' | 'release'

/** A subscription recording every settle; `failing` ops throw on their FIRST call, after it is
 * recorded (a call that threw may nonetheless have landed). */
const recording = (...failing: Op[]): { subscription: QueueSubscription; calls: string[] } => {
  const calls: string[] = []
  const left = new Set<Op>(failing)
  const record = (op: Op, text: string): void => {
    calls.push(`${op} ${text}`)
    if (left.delete(op)) throw new Error(`${op} failed`)
  }
  return {
    calls,
    subscription: {
      pull: async () => [],
      ack: async (receipts) => record('ack', receipts.join(',')),
      release: async (receipts) => record('release', receipts.join(',')),
      nack: async (entries) =>
        record('nack', entries.map((e) => `${e.receipt}@${e.delayMs}`).join(',')),
      close: async () => {},
    },
  }
}

/** Two acks, two nacks (attempts 2 and 3) and one release. */
const mixed = (): Settles => ({
  acks: ['r0', 'r1'],
  nacks: [pulled(2, 2), pulled(3, 3)],
  releases: ['r4'],
})

const runtime = (
  backoffMs?: number
): { rt: Parameters<typeof settle>[0]; scheduled: unknown[] } => {
  const scheduled: unknown[] = []
  return {
    scheduled,
    rt: {
      queue: 'jobs',
      retry: { maxAttempts: 5, ...(backoffMs !== undefined ? { backoffMs } : {}) },
      events: { retryScheduled: (ctx) => scheduled.push(ctx) },
    },
  }
}

describe('sortSettle', () => {
  it.each([
    ['ok', 'acks', { status: 'ok' }, true],
    ['ok without a policy', 'acks', { status: 'ok' }, false],
    ['a failure under a retry policy', 'nacks', { status: 'failed', error: 'x' }, true],
    ['a failure without a policy', 'releases', { status: 'failed' }, false],
    ['a timeout under a retry policy', 'nacks', { status: 'released', reason: 'timeout' }, true],
    ['a timeout without a policy', 'releases', { status: 'released', reason: 'timeout' }, false],
    ['a shutdown release', 'releases', { status: 'released', reason: 'shutdown' }, true],
    ['a message that never started', 'releases', undefined, true],
  ] as const)('%s goes to %s', (_name, kind, slot, hasRetry) => {
    const into: Settles = { acks: [], nacks: [], releases: [] }
    const leased = pulled(7)
    sortSettle(into, leased, slot as Slot, hasRetry)
    expect(into).toEqual({
      acks: [],
      nacks: [],
      releases: [],
      [kind]: [kind === 'nacks' ? leased : 'r7'],
    })
  })
})

describe('settle', () => {
  it('lands acks, then the whole failed set in ONE nack, then releases', async () => {
    const { subscription, calls } = recording()
    const { rt, scheduled } = runtime(250)
    await settle(rt, subscription, mixed())
    expect(calls).toEqual(['ack r0,r1', 'nack r2@250,r3@250', 'release r4'])
    // One event per failed delivery, with the attempt it failed on.
    expect(scheduled).toEqual([
      { queue: 'jobs', attempt: 2, delayMs: 250 },
      { queue: 'jobs', attempt: 3, delayMs: 250 },
    ])
  })

  it('issues no call for an empty kind, and the backoff defaults to no delay', async () => {
    const { subscription, calls } = recording()
    const { rt } = runtime()
    await settle(rt, subscription, { acks: [], nacks: [pulled(1)], releases: [] })
    await settle(rt, subscription, { acks: [], nacks: [], releases: [] })
    expect(calls).toEqual(['nack r1@0'])
  })

  it('an ack that threw: everything is released, the acks included, and no nack is issued', async () => {
    const { subscription, calls } = recording('ack')
    const { rt, scheduled } = runtime()
    await expect(settle(rt, subscription, mixed())).rejects.toThrow('ack failed')
    // An ack that landed is terminal (the later release is a no-op); one that did not holds a
    // lease only this worker can free promptly.
    expect(calls).toEqual(['ack r0,r1', 'release r0,r1,r2,r3,r4'])
    expect(scheduled).toEqual([])
  })

  it('a nack that threw: the nacks and the releases are released, the landed acks are not', async () => {
    const { subscription, calls } = recording('nack')
    const { rt, scheduled } = runtime()
    await expect(settle(rt, subscription, mixed())).rejects.toThrow('nack failed')
    expect(calls).toEqual(['ack r0,r1', 'nack r2@0,r3@0', 'release r2,r3,r4'])
    expect(scheduled).toEqual([]) // it may not have landed: nothing is announced
  })

  it('a release that threw is never re-issued', async () => {
    const { subscription, calls } = recording('release')
    const { rt, scheduled } = runtime()
    await expect(settle(rt, subscription, mixed())).rejects.toThrow('release failed')
    // If it landed, the adapter may already have handed the next delivery out under the
    // same receipt: releasing again would free that new delivery mid-flight.
    expect(calls).toEqual(['ack r0,r1', 'nack r2@0,r3@0', 'release r4'])
    expect(scheduled).toHaveLength(2) // the nacks before it did land
  })

  it('a compensating release that also throws is best-effort: the first error is what surfaces', async () => {
    const { subscription, calls } = recording('ack', 'release')
    const { rt } = runtime()
    await expect(settle(rt, subscription, mixed())).rejects.toThrow('ack failed')
    expect(calls).toEqual(['ack r0,r1', 'release r0,r1,r2,r3,r4'])
  })
})
