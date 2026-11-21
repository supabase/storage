type PartPermit = { release(): Promise<void> }

type Waiter = {
  resolve: (permit: PartPermit) => void
  signal?: AbortSignal
  onAbort: () => void
}

export class PartUploadSemaphore {
  private available: number
  private readonly waiters: Waiter[] = []

  constructor(count: number) {
    this.available = count
  }

  acquire(signal?: AbortSignal): Promise<PartPermit> {
    if (signal?.aborted) return Promise.reject(signal.reason)
    if (this.available > 0) {
      this.available--
      return Promise.resolve(this.createPermit())
    }

    return new Promise((resolve, reject) => {
      const waiter: Waiter = {
        resolve,
        signal,
        onAbort: () => {
          const index = this.waiters.indexOf(waiter)
          if (index !== -1) this.waiters.splice(index, 1)
          reject(signal?.reason)
        },
      }
      this.waiters.push(waiter)
      signal?.addEventListener('abort', waiter.onAbort, { once: true })
    })
  }

  private createPermit(): PartPermit {
    let released = false
    return {
      release: async () => {
        if (released) return
        released = true
        const next = this.waiters.shift()
        if (next) {
          next.signal?.removeEventListener('abort', next.onAbort)
          next.resolve(this.createPermit())
        } else {
          this.available++
        }
      },
    }
  }
}
