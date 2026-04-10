/**
 * Polls `getValue` until `predicate` is true or the timeout expires. Handy for
 * filesystem / async side effects where there's no reliable "done" event.
 */
export async function waitForEventually<T>(
  getValue: () => Promise<T> | T,
  predicate: (value: T) => boolean,
  description: string,
  timeoutMs = 5000,
  intervalMs = 25
): Promise<T> {
  const deadline = Date.now() + timeoutMs
  let lastValue: T | undefined

  while (Date.now() <= deadline) {
    lastValue = await getValue()
    if (predicate(lastValue)) return lastValue
    await new Promise((resolve) => setTimeout(resolve, intervalMs))
  }

  throw new Error(
    `Timed out after ${timeoutMs}ms waiting for ${description}. Last value: ${JSON.stringify(lastValue)}`
  )
}
