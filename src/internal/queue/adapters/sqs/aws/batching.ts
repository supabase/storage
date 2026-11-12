/** SQS batch calls take at most ten entries. */
export const MAX_BATCH_ENTRIES = 10

/** Split `items` into consecutive chunks of at most `size`. */
export const splitIntoChunks = <T>(items: readonly T[], size: number): T[][] => {
  const chunks: T[][] = []
  for (let start = 0; start < items.length; start += size) {
    chunks.push(items.slice(start, start + size))
  }
  return chunks
}

/**
 * Run `task` over `items` with at most `concurrency` calls in flight, and resolve with the
 * results in input order. After a failure no further call is started; the returned promise
 * rejects with the first failure once every started call has settled.
 */
export const mapWithConcurrency = async <T, R>(
  items: readonly T[],
  concurrency: number,
  task: (item: T, index: number) => Promise<R>
): Promise<R[]> => {
  const results = new Array<R>(items.length)
  let nextIndex = 0
  let firstFailure: { error: unknown } | undefined
  const runUntilDrained = async (): Promise<void> => {
    while (firstFailure === undefined) {
      const index = nextIndex++
      if (index >= items.length) return
      try {
        results[index] = await task(items[index] as T, index)
      } catch (error) {
        firstFailure ??= { error }
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, runUntilDrained))
  if (firstFailure !== undefined) throw firstFailure.error
  return results
}

export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms))
