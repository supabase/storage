import {
  createInvalidatableSingleFlightByKey,
  createSingleFlightByKey,
} from '@internal/concurrency'
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

describe('createInvalidatableSingleFlightByKey', () => {
  it('detaches the current flight so a later caller starts a replacement', async () => {
    const singleFlight = createInvalidatableSingleFlightByKey<string>()
    const firstWork = Promise.withResolvers<string>()
    const secondWork = Promise.withResolvers<string>()
    const work = vi
      .fn<(isCurrent: () => boolean) => Promise<string>>()
      .mockReturnValueOnce(firstWork.promise)
      .mockReturnValueOnce(secondWork.promise)

    const first = singleFlight.run('tenant-a', work)

    expect(singleFlight.has('tenant-a')).toBe(true)
    expect(singleFlight.invalidate('tenant-a')).toBe(true)
    expect(singleFlight.has('tenant-a')).toBe(false)

    const second = singleFlight.run('tenant-a', work)

    expect(work).toHaveBeenCalledTimes(2)

    firstWork.resolve('detached')
    await expect(first).resolves.toBe('detached')
    expect(singleFlight.has('tenant-a')).toBe(true)

    secondWork.resolve('current')
    await expect(second).resolves.toBe('current')
    expect(singleFlight.has('tenant-a')).toBe(false)
  })

  it('exposes generation ownership without redirecting detached callers', async () => {
    const singleFlight = createInvalidatableSingleFlightByKey<string>()
    const firstWork = Promise.withResolvers<string>()
    const secondWork = Promise.withResolvers<string>()
    let firstIsCurrent: (() => boolean) | undefined
    let secondIsCurrent: (() => boolean) | undefined

    const first = singleFlight.run('tenant-a', (isCurrent) => {
      firstIsCurrent = isCurrent
      return firstWork.promise
    })

    singleFlight.invalidate('tenant-a')

    const second = singleFlight.run('tenant-a', (isCurrent) => {
      secondIsCurrent = isCurrent
      return secondWork.promise
    })

    expect(firstIsCurrent?.()).toBe(false)
    expect(secondIsCurrent?.()).toBe(true)

    firstWork.resolve('detached')
    secondWork.resolve('current')

    await expect(first).resolves.toBe('detached')
    await expect(second).resolves.toBe('current')
  })

  it('does not let a detached rejection remove the replacement flight', async () => {
    const singleFlight = createInvalidatableSingleFlightByKey<string>()
    const firstWork = Promise.withResolvers<string>()
    const secondWork = Promise.withResolvers<string>()

    const first = singleFlight.run('tenant-a', () => firstWork.promise)
    singleFlight.invalidate('tenant-a')
    const second = singleFlight.run('tenant-a', () => secondWork.promise)

    firstWork.reject(new Error('detached failure'))

    await expect(first).rejects.toThrow('detached failure')
    expect(singleFlight.has('tenant-a')).toBe(true)

    secondWork.resolve('current')
    await expect(second).resolves.toBe('current')
  })

  it('clears a synchronously thrown current flight', async () => {
    const singleFlight = createInvalidatableSingleFlightByKey<string>()
    const failure = new Error('sync failure')

    await expect(
      singleFlight.run('tenant-a', () => {
        throw failure
      })
    ).rejects.toBe(failure)

    expect(singleFlight.has('tenant-a')).toBe(false)
    await expect(singleFlight.run('tenant-a', () => Promise.resolve('recovered'))).resolves.toBe(
      'recovered'
    )
  })
})
