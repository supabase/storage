/**
 * Memoizes a promise
 * @param func
 */
export function memoizePromise<T, Args extends unknown[]>(
  func: (...args: Args) => Promise<T>
): (...args: Args) => Promise<T> {
  const cache = new Map<string, Promise<T>>()

  function generateKey(args: Args): string {
    return args
      .map((arg) => {
        if (typeof arg === 'object' && arg !== null) {
          return Object.entries(arg).sort().toString()
        }
        return String(arg)
      })
      .join('|')
  }

  return async function (...args: Args): Promise<T> {
    const key = generateKey(args)
    if (cache.has(key)) {
      return cache.get(key)!
    }

    const result = func(...args)
    cache.set(key, result)
    return result
  }
}
