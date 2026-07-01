export function createSingleFlightByKey<T>() {
  const inFlightMap = new Map<string, Promise<T>>()

  return (key: string, fn: () => Promise<T>) => {
    const inFlight = inFlightMap.get(key)
    if (inFlight) {
      return inFlight
    }

    const { promise, resolve, reject } = Promise.withResolvers<T>()
    inFlightMap.set(key, promise)

    const deleteInFlight = () => {
      if (inFlightMap.get(key) === promise) {
        inFlightMap.delete(key)
      }
    }

    void promise.then(deleteInFlight, deleteInFlight)

    try {
      Promise.resolve(fn()).then(resolve, reject)
    } catch (e) {
      reject(e)
    }

    return promise
  }
}
