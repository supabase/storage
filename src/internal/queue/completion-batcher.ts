type Completion = {
  id: string
  resolve: () => void
  reject: (error: unknown) => void
}

// Batch completion writes and keep each job pending until acknowledged.
export class CompletionBatcher {
  private pending: Completion[] = []
  private timer?: ReturnType<typeof setTimeout>
  private draining = false

  constructor(
    private readonly completeBatch: (ids: string[]) => Promise<unknown>,
    private readonly maxSize: number,
    private readonly maxDelayMs = 50
  ) {}

  complete(id: string): Promise<void> {
    const { promise, resolve, reject } = Promise.withResolvers<void>()
    this.pending.push({ id, resolve, reject })

    if (this.draining || this.pending.length >= this.maxSize) {
      void this.flush()
    } else if (!this.timer) {
      this.timer = setTimeout(() => void this.flush(), this.maxDelayMs)
    }

    return promise
  }

  drain() {
    this.draining = true
    void this.flush()
  }

  private async flush() {
    if (this.timer) {
      clearTimeout(this.timer)
      this.timer = undefined
    }

    const batch = this.pending
    this.pending = []
    if (batch.length === 0) return

    try {
      await this.completeBatch(batch.map(({ id }) => id))
      for (const completion of batch) completion.resolve()
    } catch (error) {
      for (const completion of batch) completion.reject(error)
    }
  }
}
