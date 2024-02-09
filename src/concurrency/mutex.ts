import { Semaphore } from '@shopify/semaphore'

export function createMutexByKey() {
  const semaphoreMap = new Map<string, { semaphore: Semaphore; count: number }>()

  return async (key: string, fn: () => Promise<any>) => {
    let entry = semaphoreMap.get(key)
    if (!entry) {
      entry = { semaphore: new Semaphore(1), count: 0 }
      semaphoreMap.set(key, entry)
    }

    entry.count++
    const permit = await entry.semaphore.acquire()

    try {
      return await fn()
    } finally {
      await permit.release()
      entry.count--

      // Remove the semaphore from the map if it's no longer in use.
      if (entry.count === 0) {
        semaphoreMap.delete(key)
      }
    }
  }
}
