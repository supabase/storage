import { calculateMaxCacheSizeBytes, createConstantSizeCalculation } from './size'

describe('cache sizing helpers', () => {
  test('calculates max cache size from entry count and estimated entry size', () => {
    expect(calculateMaxCacheSizeBytes(4, 512)).toBe(2048)
  })

  test('returns a stable per-entry size without inspecting cached values', () => {
    const value = new Proxy(
      {},
      {
        get() {
          throw new Error('value should not be inspected')
        },
      }
    )
    const sizeCalculation = createConstantSizeCalculation<object, string>(512)

    expect(sizeCalculation(value, 'cache-key')).toBe(512)
  })
})
