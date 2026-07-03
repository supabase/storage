import { createSingleFlightByKey } from '@internal/concurrency'
import { vi } from 'vitest'

describe('createSingleFlightByKey', () => {
  it('shares same-key in-flight work across concurrent callers', async () => {
    const singleFlight = createSingleFlightByKey<string>()
    const firstWork = Promise.withResolvers<string>()
    const work = vi.fn().mockReturnValue(firstWork.promise)

    const first = singleFlight('tenant-a', work)
    const second = singleFlight('tenant-a', work)
    const otherKey = singleFlight('tenant-b', () => Promise.resolve('other'))

    expect(work).toHaveBeenCalledTimes(1)

    firstWork.resolve('shared')

    await expect(Promise.all([first, second, otherKey])).resolves.toEqual([
      'shared',
      'shared',
      'other',
    ])
  })

  it('clears rejected in-flight work so a later caller can retry', async () => {
    const singleFlight = createSingleFlightByKey<string>()
    const failure = new Error('lookup failed')
    const work = vi.fn().mockRejectedValueOnce(failure).mockResolvedValueOnce('recovered')

    await expect(singleFlight('tenant-a', work)).rejects.toBe(failure)
    await expect(singleFlight('tenant-a', work)).resolves.toBe('recovered')

    expect(work).toHaveBeenCalledTimes(2)
  })

  it('shares same-key in-flight rejections across concurrent callers', async () => {
    const singleFlight = createSingleFlightByKey<string>()
    const failure = new Error('shared failure')
    const firstWork = Promise.withResolvers<string>()
    const work = vi.fn().mockReturnValue(firstWork.promise)

    const first = singleFlight('tenant-a', work)
    const second = singleFlight('tenant-a', work)

    expect(work).toHaveBeenCalledTimes(1)

    firstWork.reject(failure)

    await expect(Promise.allSettled([first, second])).resolves.toEqual([
      { status: 'rejected', reason: failure },
      { status: 'rejected', reason: failure },
    ])
  })

  it('clears resolved in-flight work so later callers start new work', async () => {
    const singleFlight = createSingleFlightByKey<string>()
    const work = vi.fn().mockResolvedValueOnce('first').mockResolvedValueOnce('second')

    await expect(singleFlight('tenant-a', work)).resolves.toBe('first')
    await expect(singleFlight('tenant-a', work)).resolves.toBe('second')

    expect(work).toHaveBeenCalledTimes(2)
  })

  it('clears synchronously thrown in-flight work so later callers can retry', async () => {
    const singleFlight = createSingleFlightByKey<string>()
    const failure = new Error('sync failure')
    const work = vi.fn(() => {
      throw failure
    })

    await expect(singleFlight('tenant-a', work)).rejects.toBe(failure)
    await expect(singleFlight('tenant-a', () => Promise.resolve('recovered'))).resolves.toBe(
      'recovered'
    )

    expect(work).toHaveBeenCalledTimes(1)
  })
})
