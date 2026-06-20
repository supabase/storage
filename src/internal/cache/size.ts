function assertPositiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new TypeError(`${name} must be a positive integer`)
  }

  return value
}

export function calculateMaxCacheSizeBytes(
  maxItems: number,
  estimatedEntrySizeBytes: number
): number {
  return (
    assertPositiveInteger(maxItems, 'maxItems') *
    assertPositiveInteger(estimatedEntrySizeBytes, 'estimatedEntrySizeBytes')
  )
}

export function createConstantSizeCalculation<V, K>(
  estimatedEntrySizeBytes: number
): (value: V, key: K) => number {
  const size = assertPositiveInteger(estimatedEntrySizeBytes, 'estimatedEntrySizeBytes')

  return () => size
}
