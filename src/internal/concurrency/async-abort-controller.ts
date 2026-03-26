/**
 * This special AbortController is used to wait for all the abort handlers to finish before resolving the promise.
 */
export class AsyncAbortController extends AbortController {
  protected promises: Promise<unknown>[] = []
  protected priority = 0
  protected groups = new Map<number, AsyncAbortController[]>()

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

  protected _nextGroup?: AsyncAbortController

  get nextGroup() {
    if (!this._nextGroup) {
      this._nextGroup = new AsyncAbortController()
      this._nextGroup.priority = this.priority + 1
    }

    let existingGroups = this.groups.get(this._nextGroup.priority)
    if (!existingGroups) {
      existingGroups = []
    }

    existingGroups.push(this._nextGroup)
    this.groups.set(this._nextGroup.priority, existingGroups)
    return this._nextGroup
  }

  async abortAsync() {
    this.abort()
    while (this.promises.length > 0) {
      const promises = this.promises.splice(0, 100)
      await Promise.allSettled(promises)
    }
    await this.abortGroups()
  }

  protected async abortGroups() {
    for (const [, group] of this.groups) {
      await Promise.allSettled(group.map((g) => g.abortAsync()))
    }
  }
}
