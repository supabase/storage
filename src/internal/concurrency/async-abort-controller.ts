/**
 * This special AbortController is used to wait for all the abort handlers to finish before resolving the promise.
 */
export class AsyncAbortController extends AbortController {
  protected promises: Promise<any>[] = []
  protected priority = 0
  protected groups = new Map<number, AsyncAbortController[]>()

  constructor() {
    super()

    const originalEventListener = this.signal.addEventListener

    // Patch event addEventListener to keep track of listeners and their promises
    this.signal.addEventListener = (type: string, listener: any, options: any) => {
      if (type !== 'abort') {
        return originalEventListener.call(this.signal, type, listener, options)
      }

      let resolving: undefined | (() => Promise<void>) = undefined
      const promise = new Promise<void>(async (resolve, reject) => {
        resolving = async (): Promise<void> => {
          try {
            const result = await listener()
            resolve(result)
          } catch (e) {
            reject(e)
          }
        }
      })
      this.promises.push(promise)

      if (!resolving) {
        throw new Error('resolve is undefined')
      }

      return originalEventListener.call(this.signal, type, resolving, options)
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
