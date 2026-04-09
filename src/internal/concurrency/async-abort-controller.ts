/**
 * This special AbortController is used to wait for all the abort handlers to finish before resolving the promise.
 */
export class AsyncAbortController extends AbortController {
  protected promises: Promise<unknown>[] = []
  protected _nextGroup?: AsyncAbortController

  constructor() {
    super()

    const originalEventListener = this.signal.addEventListener.bind(this.signal)

    // Patch event addEventListener to keep track of listeners and their promises
    this.signal.addEventListener = (
      type: string,
      listener: EventListenerOrEventListenerObject | null,
      options?: boolean | AddEventListenerOptions
    ) => {
      if (!listener) {
        return
      }

      if (type !== 'abort') {
        return originalEventListener(type, listener, options)
      }

      let resolving: ((event: Event) => Promise<void>) | undefined
      const promise = new Promise<void>((resolve, reject) => {
        resolving = (event: Event): Promise<void> => {
          try {
            const result =
              typeof listener === 'function'
                ? listener.call(this.signal, event)
                : listener.handleEvent(event)

            return Promise.resolve(result).then(() => {
              resolve()
            }, reject)
          } catch (error) {
            reject(error)
            return Promise.resolve()
          }
        }
      })
      this.promises.push(promise)

      if (!resolving) {
        throw new Error('resolve is undefined')
      }

      return originalEventListener(type, resolving as EventListener, options)
    }
  }

  get nextGroup() {
    if (this._nextGroup) {
      return this._nextGroup
    }

    this._nextGroup = new AsyncAbortController()
    return this._nextGroup
  }

  async abortAsync() {
    this.abort()
    while (this.promises.length > 0) {
      const promises = this.promises.splice(0, 100)
      await Promise.allSettled(promises)
    }
    await this.abortNextGroup()
  }

  protected async abortNextGroup() {
    if (this._nextGroup) {
      await this._nextGroup.abortAsync()
    }
  }
}
