import { AsyncLocalStorage } from 'node:async_hooks'
import type { IncomingMessage } from 'node:http'
import { logSchema, type RequestLogContext } from '@internal/monitoring/logger'
import type { FastifyBaseLogger } from 'fastify'

type RemovableStore = {
  remove(id: string): Promise<void>
}

type TusBackend = 'file' | 's3'
type InvalidationReason =
  | 'aborted'
  | 'cancelled'
  | 'incomplete'
  | 'length_mismatch'
  | 'offset_mismatch'
  | 'write_error'

type TusRequest = IncomingMessage & {
  log?: FastifyBaseLogger
  upload?: RequestLogContext
}

type TusRequestState = {
  request: TusRequest
  cancellationSignal?: AbortSignal
  writeDidMutate?: boolean
}

type WriteCompletionOptions = {
  trackedMutations?: boolean
}

// Upstream closes its datastore proxy with a clean EOF when a request is
// cancelled. Keep the original Node request available to the datastore so a
// truncated HTTP message cannot be mistaken for a successful partial write.
const requestStorage = new AsyncLocalStorage<TusRequestState>()

class IncompleteTusRequestError extends Error {
  readonly status_code = 400
  readonly body = 'Upload request body was incomplete\n'

  constructor() {
    super('TUS upload request body was incomplete')
    this.name = 'IncompleteTusRequestError'
  }
}

export function runWithTusRequest<T>(request: IncomingMessage, callback: () => T): T {
  return requestStorage.run({ request }, callback)
}

// Makes upstream internal cancellation observable to datastore guards.
export function setTusRequestCancellationSignal(signal: AbortSignal): void {
  const state = requestStorage.getStore()
  if (state) {
    state.cancellationSignal = signal
  }
}

// Marks the first backend operation that can change durable upload state.
export function markTusWriteMutation(): void {
  const state = requestStorage.getStore()
  if (state) {
    state.writeDidMutate = true
  }
}

export async function writeWithRequestCompletion(
  store: RemovableStore,
  backend: TusBackend,
  id: string,
  offset: number,
  write: () => Promise<number>,
  options: WriteCompletionOptions = {}
): Promise<number> {
  const state = requestStorage.getStore()

  if (!state) {
    return write()
  }

  state.writeDidMutate = false

  let newOffset: number
  try {
    newOffset = await write()
  } catch (error) {
    if (options.trackedMutations && !state.writeDidMutate) {
      throw error
    }

    return invalidateUpload(state.request, store, backend, id, 'write_error', error)
  }

  const reason = getInvalidationReason(state, offset, newOffset)
  if (!reason) {
    return newOffset
  }

  return invalidateUpload(
    state.request,
    store,
    backend,
    id,
    reason,
    new IncompleteTusRequestError()
  )
}

function getInvalidationReason(
  state: TusRequestState,
  offset: number,
  newOffset: number
): InvalidationReason | undefined {
  const { request } = state

  if (
    !Number.isSafeInteger(offset) ||
    !Number.isSafeInteger(newOffset) ||
    offset < 0 ||
    newOffset < offset
  ) {
    return 'offset_mismatch'
  }

  const bytesWritten = newOffset - offset

  const contentLength = request.headers['content-length']
  if (contentLength !== undefined) {
    if (typeof contentLength !== 'string' || !/^\d+$/.test(contentLength)) {
      return 'length_mismatch'
    }

    const expectedBytes = Number(contentLength)
    return Number.isSafeInteger(bytesWritten) &&
      Number.isSafeInteger(expectedBytes) &&
      bytesWritten === expectedBytes
      ? undefined
      : 'length_mismatch'
  }

  if (request.aborted) {
    return 'aborted'
  }

  if (!request.complete) {
    return 'incomplete'
  }

  return state.cancellationSignal?.aborted ? 'cancelled' : undefined
}

async function invalidateUpload(
  request: TusRequest,
  store: RemovableStore,
  backend: TusBackend,
  id: string,
  reason: InvalidationReason,
  cause: unknown
): Promise<never> {
  let error = cause
  let outcome: 'success' | 'cleanup_error' = 'success'

  try {
    await store.remove(id)
  } catch (cleanupError) {
    error = new AggregateError(
      [cause, cleanupError],
      `Failed to invalidate incomplete TUS upload ${id}`
    )
    outcome = 'cleanup_error'
  }

  if (request.log) {
    logSchema.warning(request.log, 'TUS upload invalidated', {
      type: 'tus',
      tenantId: request.upload?.tenantId,
      project: request.upload?.tenantId,
      reqId: request.upload?.reqId,
      sbReqId: request.upload?.sbReqId,
      error,
      metadata: JSON.stringify({ backend, reason, outcome }),
    })
  }

  throw error
}
