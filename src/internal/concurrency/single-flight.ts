export type InvalidatableSingleFlightByKey<T> = {
  run(key: string, load: (isCurrent: () => boolean) => Promise<T>): Promise<T>
  has(key: string): boolean
  invalidate(key: string): boolean
}

export function createInvalidatableSingleFlightByKey<T>(): InvalidatableSingleFlightByKey<T> {
  const inFlightMap = new Map<string, Promise<T>>()

  return {
    run(key, load) {
      const inFlight = inFlightMap.get(key)
      if (inFlight) {
        return inFlight
      }

      const { promise, resolve, reject } = Promise.withResolvers<T>()
      inFlightMap.set(key, promise)

      const isCurrent = () => inFlightMap.get(key) === promise
      const deleteInFlight = () => {
        if (isCurrent()) {
          inFlightMap.delete(key)
        }
      }

      void promise.then(deleteInFlight, deleteInFlight)

      try {
        void Promise.resolve(load(isCurrent)).then(resolve, reject)
      } catch (error) {
        reject(error)
      }

      return promise
    },
    has(key) {
      return inFlightMap.has(key)
    },
    invalidate(key) {
      return inFlightMap.delete(key)
    },
  }
}

export function createSingleFlightByKey<T>() {
  const singleFlight = createInvalidatableSingleFlightByKey<T>()
  return (key: string, load: () => Promise<T>) => singleFlight.run(key, load)
}
