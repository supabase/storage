import { DatabaseError, type Pool, type PoolClient } from 'pg'

const disposeClientOnRelease = Symbol('disposeClientOnRelease')

type DisposableQueryError = Error & {
  [disposeClientOnRelease]?: true
}

export function createAbortError(): Error & { code: string } {
  const error = new Error('Query was aborted') as Error & { code: string }
  error.name = 'AbortError'
  error.code = 'ABORT_ERR'
  return error
}

export function shouldDisposeClient(error: unknown): boolean {
  return (
    error instanceof Error &&
    (error.name === 'AbortError' ||
      (error as DisposableQueryError)[disposeClientOnRelease] === true)
  )
}

export function markClientDisposable(error: unknown): void {
  if (error instanceof Error) {
    // The pool release path reads this marker from the exact Error instance
    // thrown by the query. Do not wrap or replace it before releasing the client.
    const disposableError = error as DisposableQueryError
    disposableError[disposeClientOnRelease] = true
  }
}

export function isConnectionStateError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false
  }

  if (error instanceof DatabaseError) {
    return error.code ? error.code.startsWith('08') : isPgProtocolError(error)
  }

  return isPgProtocolError(error)
}

function isPgProtocolError(error: Error): boolean {
  return (
    error.message.startsWith('received invalid response:') ||
    error.message.startsWith('Received unexpected ') ||
    error.message.startsWith('Unknown authenticationOk message type')
  )
}

// Socket-level failures of a dead pooled connection can surface as plain Errors.
// No setup has run yet, so a fresh client can safely retry. Connection-establishment
// failures such as ECONNREFUSED and connect timeouts are deliberately excluded.
export function isBrokenClientError(error: unknown): boolean {
  if (!(error instanceof Error) || error.name === 'AbortError') {
    return false
  }

  if ((error as DisposableQueryError)[disposeClientOnRelease] === true) {
    return true
  }

  const code = (error as NodeJS.ErrnoException).code
  return (
    code === 'ECONNRESET' ||
    code === 'EPIPE' ||
    error.message === 'Connection terminated unexpectedly' ||
    error.message === 'Client has encountered a connection error and is not queryable'
  )
}

export function isConnectionTimeoutError(error: unknown): error is Error {
  return (
    error instanceof Error &&
    (error.message === 'timeout expired' ||
      error.message === 'timeout exceeded when trying to connect' ||
      error.message === 'Connection terminated due to connection timeout')
  )
}

export function isConnectionLimitError(error: unknown): boolean {
  // PgBouncer can report connection limits as 08P01 protocol_violation. That
  // intentionally overlaps isConnectionStateError so failed clients are both
  // retried and disposed instead of being returned to the pool.
  return (
    error instanceof DatabaseError &&
    ((error.code === '08P01' && error.message.includes('no more connections allowed')) ||
      error.message.includes('Max client connections reached'))
  )
}

export function isRetryableTransactionSetupError(error: unknown): boolean {
  return (
    isConnectionStateError(error) || isConnectionLimitError(error) || isBrokenClientError(error)
  )
}

export type PoolErrorHandler = (error: Error) => void

export function attachPoolErrorHandler<T extends Pool>(pool: T, onError: PoolErrorHandler): T {
  pool.on('error', onError)
  return pool
}

function ensureError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}

export class PgClientCheckout {
  private clientError?: Error
  private released = false

  constructor(
    readonly client: PoolClient,
    private readonly onClientError?: (error: Error) => void
  ) {
    this.client.on('error', this.onError)
  }

  get error(): Error | undefined {
    return this.clientError
  }

  throwIfErrored(): void {
    if (this.clientError) {
      throw this.clientError
    }
  }

  releaseErrorForQuery(error: unknown): Error | undefined {
    if (this.clientError) {
      return this.clientError
    }

    if (shouldDisposeClient(error)) {
      return ensureError(error)
    }

    return undefined
  }

  releaseErrorForFinalizer(error: unknown): Error {
    return this.clientError ?? ensureError(error)
  }

  release(error?: Error): void {
    if (this.released) {
      return
    }
    this.released = true

    const disposalError = error ?? this.clientError
    try {
      this.client.release(disposalError)

      if (disposalError && this.client.pipeline) {
        // pg waits for pipelined queries in end(). Discarded clients must close
        // immediately so a failed cancel cannot block queued pool checkouts.
        this.client.connection.stream.destroy()
      }
    } finally {
      this.client.off('error', this.onError)
    }
  }

  private readonly onError = (error: unknown): void => {
    const normalizedError = ensureError(error)
    markClientDisposable(normalizedError)

    if (!this.clientError) {
      this.clientError = normalizedError
    }

    this.onClientError?.(normalizedError)
  }
}

// pg-pool drops its idle error listener and resolves the checkout in the same tick so a
// listener added after connect can miss an error emitted in between.
export async function checkoutClient(
  pool: Pool,
  onError?: (error: Error) => void
): Promise<PgClientCheckout> {
  try {
    return await new Promise<PgClientCheckout>((resolve, reject) => {
      pool.connect((error, client) => {
        if (error || !client) {
          reject(error ?? new Error('Pool checkout returned no client'))
          return
        }
        resolve(new PgClientCheckout(client, onError))
      })
    })
  } catch (error) {
    if (error instanceof Error) {
      Error.captureStackTrace(error, checkoutClient)
    }
    throw error
  }
}
