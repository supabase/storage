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

      const event = new Event('abort')
      let resolving: undefined | (() => Promise<void>) = undefined
      const promise = new Promise<void>((resolve, reject) => {
        resolving = async (): Promise<void> => {
          return Promise.resolve()
            .then(() => {
              if (typeof listener === 'function') {
                return listener.call(this.signal, event)
              }
              return listener.handleEvent(event)
            })
            .then(() => {
              resolve()
            })
            .catch((error) => {
              reject(error)
            })
        }
      })
      this.promises.push(promise)

      if (!resolving) {
        throw new Error('resolve is undefined')
      }

      return originalEventListener(type, resolving, options)
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
