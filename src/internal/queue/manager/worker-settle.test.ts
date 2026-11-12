import type { QueueSession } from './index'
import { pulled } from './test-helpers'
import type { Slot } from './worker/invocation'
import { type Settles, settle, sortSettle } from './worker/settle'

type Op = 'ack' | 'retry' | 'nack'

/** A session recording every settle; `failing` ops throw on their FIRST call, after it is
 * recorded (a call that threw may nonetheless have landed). */
const recording = (...failing: Op[]): { session: QueueSession; calls: string[] } => {
  const calls: string[] = []
  const left = new Set<Op>(failing)
  const record = (op: Op, text: string): void => {
    calls.push(`${op} ${text}`)
    if (left.delete(op)) throw new Error(`${op} failed`)
  }
  return {
    calls,
    session: {
      pull: async () => [],
      ack: async (receipts) => record('ack', receipts.join(',')),
      nack: async (receipts) => record('nack', receipts.join(',')),
      retry: async (entries) =>
        record('retry', entries.map((e) => `${e.receipt}@${e.delayMs}`).join(',')),
      close: async () => {},
    },
  }
}

/** Two acks, two retries (attempts 2 and 3) and one release. */
const mixed = (): Settles => ({
  acks: ['r0', 'r1'],
  retries: [pulled(2, 2), pulled(3, 3)],
  nacks: ['r4'],
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
    ['a failure under a retry policy', 'retries', { status: 'failed', error: 'x' }, true],
    ['a failure without a policy', 'nacks', { status: 'failed' }, false],
    ['a timeout release', 'nacks', { status: 'released', reason: 'timeout' }, true],
    ['a shutdown release', 'nacks', { status: 'released', reason: 'shutdown' }, true],
    ['a message that never started', 'nacks', undefined, true],
  ] as const)('%s goes to %s', (_name, kind, slot, hasRetry) => {
    const into: Settles = { acks: [], retries: [], nacks: [] }
    const leased = pulled(7)
    sortSettle(into, leased, slot as Slot, hasRetry)
    expect(into).toEqual({
      acks: [],
      retries: [],
      nacks: [],
      [kind]: [kind === 'retries' ? leased : 'r7'],
    })
  })
})

describe('settle', () => {
  it('lands acks, then the whole failed set in ONE retry, then releases', async () => {
    const { session, calls } = recording()
    const { rt, scheduled } = runtime(250)
    await settle(rt, session, mixed())
    expect(calls).toEqual(['ack r0,r1', 'retry r2@250,r3@250', 'nack r4'])
    // One event per failed delivery, with the attempt it failed on.
    expect(scheduled).toEqual([
      { queue: 'jobs', attempt: 2, delayMs: 250 },
      { queue: 'jobs', attempt: 3, delayMs: 250 },
    ])
  })

  it('issues no call for an empty kind, and the backoff defaults to no delay', async () => {
    const { session, calls } = recording()
    const { rt } = runtime()
    await settle(rt, session, { acks: [], retries: [pulled(1)], nacks: [] })
    await settle(rt, session, { acks: [], retries: [], nacks: [] })
    expect(calls).toEqual(['retry r1@0'])
  })

  it('an ack that threw: everything is released, the acks included, and no retry is issued', async () => {
    const { session, calls } = recording('ack')
    const { rt, scheduled } = runtime()
    await expect(settle(rt, session, mixed())).rejects.toThrow('ack failed')
    // An ack that landed is terminal (the later nack is a no-op); one that did not holds a
    // lease only this worker can free promptly.
    expect(calls).toEqual(['ack r0,r1', 'nack r0,r1,r2,r3,r4'])
    expect(scheduled).toEqual([])
  })

  it('a retry that threw: the retries and the releases are released, the landed acks are not', async () => {
    const { session, calls } = recording('retry')
    const { rt, scheduled } = runtime()
    await expect(settle(rt, session, mixed())).rejects.toThrow('retry failed')
    expect(calls).toEqual(['ack r0,r1', 'retry r2@0,r3@0', 'nack r2,r3,r4'])
    expect(scheduled).toEqual([]) // it may not have landed: nothing is announced
  })

  it('a nack that threw is never re-issued', async () => {
    const { session, calls } = recording('nack')
    const { rt, scheduled } = runtime()
    await expect(settle(rt, session, mixed())).rejects.toThrow('nack failed')
    // If it landed, the adapter may already have handed the next delivery out under the
    // same receipt: nacking again would release that new delivery mid-flight.
    expect(calls).toEqual(['ack r0,r1', 'retry r2@0,r3@0', 'nack r4'])
    expect(scheduled).toHaveLength(2) // the retries before it did land
  })

  it('a compensating release that also throws is best-effort: the first error is what surfaces', async () => {
    const { session, calls } = recording('ack', 'nack')
    const { rt } = runtime()
    await expect(settle(rt, session, mixed())).rejects.toThrow('ack failed')
    expect(calls).toEqual(['ack r0,r1', 'nack r0,r1,r2,r3,r4'])
  })
})
