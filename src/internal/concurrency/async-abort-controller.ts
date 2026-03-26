/**
 * This special AbortController is used to wait for all the abort handlers to finish before resolving the promise.
 */
export class AsyncAbortController extends AbortController {
  protected promises: Promise<any>[] = []
  protected _nextGroup?: AsyncAbortController

  constructor() {
    super()

    const originalEventListener = this.signal.addEventListener

    // Patch event addEventListener to keep track of listeners and their promises
    this.signal.addEventListener = (type: string, listener: any, options: any) => {
      if (type !== 'abort') {
        return originalEventListener.call(this.signal, type, listener, options)
      }

      let resolving: undefined | (() => Promise<void>) = undefined
      const promise = new Promise<void>((resolve, reject) => {
        resolving = async (): Promise<void> => {
          return Promise.resolve()
            .then(() => listener())
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

      return originalEventListener.call(this.signal, type, resolving, options)
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
