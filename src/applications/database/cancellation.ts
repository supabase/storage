import { type CancellableClient, cancelQuery } from '@internal/database/postgres/cancellation'
import { cancelledError } from './errors.js'

// Connections a PostgreSQL cancel request was sent on. The cancel travels over a connection
// of its own and can land after the statement it was meant for has finished, interrupting
// whatever runs next on that backend. Such a connection is closed when it is released
// instead of going back to the pool, so the next request's statement is never the one hit.
const cancelSentOn = new WeakSet<object>()

/**
 * A request a storage worker can cancel (database.cancel). Once cancelled, its signal is
 * aborted: a wait for a connection stops, no further statement starts for it, and the
 * statement it is running is cancelled in PostgreSQL.
 */
export class InFlightOperation {
  readonly #controller = new AbortController()
  #client?: CancellableClient

  constructor(readonly lockId?: string) {}

  get signal(): AbortSignal {
    return this.#controller.signal
  }

  get cancelled(): boolean {
    return this.#controller.signal.aborted
  }

  throwIfCancelled(): void {
    if (this.cancelled) {
      throw cancelledError()
    }
  }

  /**
   * Called before each statement the request runs on `client`: throws if the request was
   * cancelled, so the statement does not start, and otherwise makes `client` the
   * connection a cancel interrupts.
   */
  runOn(client: CancellableClient): void {
    this.throwIfCancelled()
    this.#client = client
  }

  /** Aborts the signal and returns the connection a statement of the request may be running on. */
  cancel(): CancellableClient | undefined {
    this.#controller.abort()
    return this.#client
  }
}

export class CancellationRegistry {
  private readonly operations = new Map<string, InFlightOperation>()

  /** A request without an ID cannot be cancelled, but still gets an operation to check. */
  start(requestId: string | undefined, lockId?: string): InFlightOperation {
    const operation = new InFlightOperation(lockId)
    if (requestId) {
      this.operations.set(requestId, operation)
    }
    return operation
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

    const client = operation.cancel()
    if (client) {
      cancelSentOn.add(client)
      await cancelQuery(client)
    }

    return { cancelled: true }
  }
}

/**
 * The error to release `client` with: `error` when there is one, and otherwise an error
 * when a cancel was sent on the connection, so the pool closes it instead of reusing it.
 */
export function releaseErrorFor(client: object, error?: Error): Error | undefined {
  if (error || !cancelSentOn.has(client)) {
    return error
  }
  return new Error('A cancel request was sent on this connection')
}
