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

/**
 * The delivery behind each receipt (`<jobId>:<deliveredAttempt>`) as pg-boss names an
 * attempt, each delivery once. Anything that does not parse is dropped: a junk receipt is a
 * harmless no-op and must never reach Postgres as a uuid.
 */
export const attemptsOf = (receipts: readonly string[]): JobAttempt[] => {
  const attempts = new Map<string, JobAttempt>()
  for (const receipt of receipts) {
    const at = receipt.lastIndexOf(':')
    const id = receipt.slice(0, Math.max(0, at))
    const attempt = Number(receipt.slice(at + 1))
    if (UUID.test(id) && Number.isInteger(attempt) && attempt >= 1) {
      attempts.set(receipt, { id, retryCount: attempt - 1 })
    }
  }
  return [...attempts.values()]
}

export const toPullMessage = <W>(job: JobWithMetadata<unknown>): PullMessage<W> => {
  const data = job.data
  const enveloped = isEnvelope<W>(data)
  // pg-boss bumps `retry_count` on every re-fetch, so the attempt is the delivery ordinal:
  // nack, crash expiry and retry all advance it.
  const attempt = 1 + job.retryCount
  return {
    id: job.id,
    data: enveloped ? data.p : (data as W),
    headers: enveloped ? (data.h ?? {}) : {},
    timestamp: job.createdOn.getTime(),
    attempt,
    receipt: `${job.id}:${attempt}`,
    ...(enveloped && data.k !== undefined ? { key: data.k } : {}),
  }
}
