import { describe, expect, it } from 'vitest'

describe('memoizePromise helper', () => {
  const memoizePromise = <T extends any[], R>(
    fn: (...args: T) => Promise<R>
  ): ((...args: T) => Promise<R>) => {
    const cache = new Map<string, Promise<R>>()

    return async (...args: T): Promise<R> => {
      const key = JSON.stringify(args)
      if (cache.has(key)) return cache.get(key)!
      const promise = fn(...args)
      cache.set(key, promise)
      return promise
    }
  }

  it('should cache promise results correctly', async () => {
    let callCount = 0
    const testFunction = async (arg: string): Promise<string> => {
      callCount++
      return `result-${arg}-${callCount}`
    }

    const memoizedFunction = memoizePromise(testFunction)

    const result1 = await memoizedFunction('test')
    expect(result1).toBe('result-test-1')
    expect(callCount).toBe(1)

    const result2 = await memoizedFunction('test')
    expect(result2).toBe('result-test-1')
    expect(callCount).toBe(1)

    const result3 = await memoizedFunction('different')
    expect(result3).toBe('result-different-2')
    expect(callCount).toBe(2)
  })

  it('should handle different argument combinations', async () => {
    let callCount = 0
    const testFunction = async (arg1: string, arg2: number): Promise<string> => {
      callCount++
      return `${arg1}-${arg2}-${callCount}`
    }

    const memoizedFunction = memoizePromise(testFunction)

    const result1 = await memoizedFunction('a', 1)
    const result2 = await memoizedFunction('b', 2)
    const result3 = await memoizedFunction('a', 1)

    expect(result1).toBe('a-1-1')
    expect(result2).toBe('b-2-2')
    expect(result3).toBe('a-1-1')
    expect(callCount).toBe(2)
  })

  it('should generate keys for objects and primitives', async () => {
    let callCount = 0
    const testFunction = async (obj: { name: string }, num: number): Promise<string> => {
      callCount++
      return `${obj.name}-${num}-${callCount}`
    }

    const memoizedFunction = memoizePromise(testFunction)

    const obj1 = { name: 'test' }
    const obj2 = { name: 'test' }

    const result1 = await memoizedFunction(obj1, 1)
    const result2 = await memoizedFunction(obj2, 1)

    expect(result1).toBe('test-1-1')
    expect(result2).toBe('test-1-1')
    expect(callCount).toBe(1)
  })

  it('should handle promise rejections correctly', async () => {
    let callCount = 0
    const testFunction = async (shouldFail: boolean): Promise<string> => {
      callCount++
      if (shouldFail) throw new Error('Test error')
      return `success-${callCount}`
    }

    const memoizedFunction = memoizePromise(testFunction)

    await expect(memoizedFunction(true)).rejects.toThrow('Test error')
    expect(callCount).toBe(1)

    await expect(memoizedFunction(true)).rejects.toThrow('Test error')
    expect(callCount).toBe(1)

    const result = await memoizedFunction(false)
    expect(result).toBe('success-2')
    expect(callCount).toBe(2)
  })
})
