import { DatabaseError, type Pool } from 'pg'

const disposeClientOnRelease = Symbol('disposeClientOnRelease')

type DisposableQueryError = Error & {
  [disposeClientOnRelease]?: true
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
    // PgPoolExecutor.shouldDisposeClient reads this marker from the exact Error instance
    // thrown by runPgQuery. Do not wrap or replace the error before the pool release path.
    const disposableError = error as DisposableQueryError
    disposableError[disposeClientOnRelease] = true
  }
}

export function isConnectionStateError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false
  }

  const code = (error as NodeJS.ErrnoException).code
  return (
    (typeof code === 'string' && code.startsWith('08')) ||
    error.message.startsWith('received invalid response:') ||
    error.message.startsWith('Received unexpected ') ||
    error.message.startsWith('Unknown authenticationOk message type')
  )
}

// Socket-level of a dead pooled connection can surface as a plain Error.
// No setup is run yet, so a fresh client can safely retry.
// Connection-establishment failures (ECONNREFUSED, connect timeouts) aren't retried.
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
  // intentionally overlaps isConnectionStateError so these failed clients are
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

class AbortError extends Error {
  readonly code = 'ABORT_ERR'

  constructor() {
    super('Query was aborted')
    this.name = 'AbortError'
  }
}
export const ABORT_ERROR = new AbortError()
