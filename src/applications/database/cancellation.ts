import { CancellableClient, cancelQuery } from '@internal/database/postgres/cancellation'

export type InFlightOperation = {
  client?: CancellableClient
  lockId?: string
  cancelled: boolean
}

export class CancellationRegistry {
  private readonly operations = new Map<string, InFlightOperation>()

  start(requestId: string | undefined, operation: InFlightOperation): void {
    if (requestId) {
      this.operations.set(requestId, operation)
    }
  }

  setClient(requestId: string | undefined, client: CancellableClient): void {
    if (!requestId) {
      return
    }

    const operation = this.operations.get(requestId)
    if (operation) {
      operation.client = client
    }
  }

  finish(requestId: string | undefined): void {
    if (requestId) {
      this.operations.delete(requestId)
    }
  }

  async cancel(requestId: string, lockId?: string): Promise<{ cancelled: boolean }> {
    const operation = this.operations.get(requestId)
    if (!operation) {
      return { cancelled: false }
    }

    if (lockId && operation.lockId && operation.lockId !== lockId) {
      return { cancelled: false }
    }

    operation.cancelled = true
    if (operation.client) {
      await cancelQuery(operation.client)
    }

    return { cancelled: true }
  }
}
