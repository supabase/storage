import { vi } from 'vitest'

// The round itself runs against a real database (src/test/queue/move-jobs-sqs.test.ts); only
// the lane's pacing is checked here, so the queue and database graph stays out.
vi.mock('../base', () => ({
  storageEvent: (opts: { type: string }) =>
    class {
      static readonly eventType = opts.type
      constructor(readonly data: unknown) {}
    },
}))
vi.mock('./move-jobs', () => ({ PGBOSS_V1_SCHEMA: 'pgboss_v10' }))

import { nextRoundDelay } from './move-jobs-sqs'

const lane = { delayMs: 5_000, expiresAt: 1_000_000 }
const result = (moved: number, remaining: boolean) => ({ moved, unsent: 0, remaining })

describe('nextRoundDelay', () => {
  it('paces a lane that is moving jobs at its configured delay', () => {
    expect(nextRoundDelay(lane, result(500, true), 0)).toBe(5_000)
  })

  it('comes back less often when a round found nothing it could take yet', () => {
    // Jobs remain, but none was due, free of another lane, or off a v1 worker.
    expect(nextRoundDelay(lane, result(0, true), 0)).toBe(60_000)
    expect(nextRoundDelay({ ...lane, delayMs: 120_000 }, result(0, true), 0)).toBe(120_000)
  })

  it('ends the lane when nothing movable is left', () => {
    expect(nextRoundDelay(lane, result(500, false), 0)).toBeUndefined()
    expect(nextRoundDelay(lane, result(0, false), 0)).toBeUndefined()
  })

  it('ends the lane at its time limit, whatever is left', () => {
    expect(nextRoundDelay(lane, result(500, true), 1_000_000)).toBeUndefined()
  })

  it('never asks for a longer pause than SQS can delay a message', () => {
    expect(nextRoundDelay({ ...lane, delayMs: 3_600_000 }, result(1, true), 0)).toBe(900_000)
  })
})
