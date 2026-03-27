export interface Deferred<T> {
  promise: Promise<T>
  resolve(value: T | PromiseLike<T>): void
  reject(reason?: unknown): void
}

export interface VoidDeferred {
  promise: Promise<void>
  resolve(value?: void | PromiseLike<void>): void
  reject(reason?: unknown): void
}

type DeferredFor<T> = [T] extends [void] ? VoidDeferred : Deferred<T>

export function createDeferred<T = void>(): DeferredFor<T> {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void

  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })

  return { promise, resolve, reject } as DeferredFor<T>
}
