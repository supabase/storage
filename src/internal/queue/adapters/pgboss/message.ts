import type { JobAttempt, JobWithMetadata } from 'pg-boss'
import type { ProduceMessage, PullMessage } from '../../manager/message'

/**
 * The envelope stored as `job.data`: the message fields the job table has no columns for.
 * Always written on send. A job WITHOUT the marker was produced by plain pg-boss and delivers
 * as a bare payload with empty headers.
 */
const MARKER = '__q'

interface Envelope<W> {
  readonly [MARKER]: 1
  /** The codec-encoded payload. */
  readonly p: W
  readonly h: Readonly<Record<string, string>>
  readonly k?: string
}

export const wrap = <W>(message: ProduceMessage<W>): Envelope<W> => ({
  [MARKER]: 1,
  p: message.data,
  h: message.headers ?? {},
  ...(message.key !== undefined ? { k: message.key } : {}),
})

const isEnvelope = <W>(data: unknown): data is Envelope<W> =>
  typeof data === 'object' && data !== null && (data as Record<string, unknown>)[MARKER] === 1

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const INTEGER = /^\d{1,15}$/

/**
 * One delivery of a job, as a receipt names it: `<jobId>:<attempt>:<startedMs>`. `id` and
 * `retryCount` are how pg-boss names an attempt. `startedMs` (the fetch's `started_on`, in
 * epoch milliseconds) tells apart two deliveries of the SAME attempt number, which a release
 * produces because it does not advance the attempt.
 */
export interface Delivery extends JobAttempt {
  readonly startedMs: number
}

/**
 * The deliveries a batch of receipts names, each once. Anything that does not parse is
 * dropped: a junk receipt is a harmless no-op and must never reach Postgres as a uuid.
 */
export const deliveriesOf = (receipts: readonly string[]): Delivery[] => {
  const deliveries = new Map<string, Delivery>()
  for (const receipt of receipts) {
    const [id, attempt, startedMs, ...rest] = receipt.split(':')
    if (rest.length > 0 || !UUID.test(id) || !INTEGER.test(attempt ?? '')) continue
    if (!INTEGER.test(startedMs ?? '') || Number(attempt) < 1) continue
    deliveries.set(receipt.toLowerCase(), {
      id: id.toLowerCase(),
      retryCount: Number(attempt) - 1,
      startedMs: Number(startedMs),
    })
  }
  return [...deliveries.values()]
}

export const toPullMessage = <W>(job: JobWithMetadata<unknown>): PullMessage<W> => {
  const data = job.data
  const enveloped = isEnvelope<W>(data)
  // pg-boss bumps `retry_count` when it re-fetches a job that failed or expired: a nack and
  // a crash expiry advance the attempt, a release does not.
  const attempt = 1 + job.retryCount
  return {
    id: job.id,
    data: enveloped ? data.p : (data as W),
    headers: enveloped ? (data.h ?? {}) : {},
    timestamp: job.createdOn.getTime(),
    attempt,
    receipt: `${job.id}:${attempt}:${job.startedOn.getTime()}`,
    ...(enveloped && data.k !== undefined ? { key: data.k } : {}),
  }
}
