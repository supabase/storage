import { columnsOf } from './queues'

describe('columnsOf', () => {
  it('writes a fixed retry policy with no backoff', () => {
    const columns = columnsOf({}, { retry: { maxAttempts: 4, backoffMs: 5_000 } })

    expect(columns).toMatchObject({ retryLimit: 3, retryDelay: 5, retryBackoff: false })
    // pg-boss rejects a ceiling on a queue without backoff, so the key must be absent.
    expect(columns).not.toHaveProperty('retryDelayMax')
  })

  it('writes an exponential policy as pg-boss backoff with its ceiling', () => {
    // What pg-boss applies when it fails a job itself: a crashed consumer, an expired attempt.
    const columns = columnsOf(
      {},
      {
        retry: {
          maxAttempts: 21,
          backoff: 'exponential',
          backoffMs: 1_000,
          maxBackoffMs: 32_768_000,
        },
      }
    )

    expect(columns).toMatchObject({
      retryLimit: 20,
      retryDelay: 1,
      retryBackoff: true,
      retryDelayMax: 32_768,
    })
  })

  it('leaves the ceiling out of an exponential policy that declares none', () => {
    const columns = columnsOf(
      {},
      { retry: { maxAttempts: 3, backoff: 'exponential', backoffMs: 2_000 } }
    )

    expect(columns).toMatchObject({ retryDelay: 2, retryBackoff: true })
    expect(columns).not.toHaveProperty('retryDelayMax')
  })

  it('treats an exponential policy with no base delay as no backoff', () => {
    expect(columnsOf({}, { retry: { maxAttempts: 3, backoff: 'exponential' } })).toMatchObject({
      retryDelay: 0,
      retryBackoff: false,
    })
  })
})
