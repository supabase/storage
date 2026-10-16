import { subscribe } from 'node:diagnostics_channel'
import { Readable } from 'node:stream'
import {
  httpPoolBusySockets,
  httpPoolErrors,
  httpPoolFreeSockets,
  httpPoolPendingRequests,
} from '@internal/monitoring/metrics'
import { Agent, type Dispatcher, errors } from 'undici'
import { getConfig } from '../../config'

const { region } = getConfig()

const MAX_WRITE_CHUNK = 64 * 1024

// Undici fails the in-flight request with the same error it later emits on 'disconnect'.
const requestErrors = new WeakSet<Error>()
subscribe('undici:request:error', (message) => {
  const { error } = message as { error: unknown }
  if (error instanceof Error) requestErrors.add(error)
})

export interface InstrumentedAgent {
  dispatcher: Dispatcher
  monitor: () => NodeJS.Timeout
  close: () => Promise<void>
}

interface AgentStats {
  errorSocketCount: number
  timeoutSocketCount: number
  connectErrorCount: number
  busySocketCount: number
  freeSocketCount: number
  pendingRequestCount: number
}

export interface AgentOptions {
  maxSockets: number
  /** Header, response-body and upload-input timeout in milliseconds; zero disables header and upload timers and idles bodies out after 30 s. */
  requestTimeoutMs?: number
}

/**
 * Creates an instrumented undici Agent.
 * Tracks connection failures, socket errors and inactivity timeouts.
 */
export function createAgent(name: string, options: AgentOptions): InstrumentedAgent {
  const requestTimeoutMs = options.requestTimeoutMs ?? 0
  const agent = new Agent({
    connections: options.maxSockets,
    keepAliveTimeout: 15_000,
    pipelining: 1,
    headersTimeout: requestTimeoutMs,
    bodyTimeout: requestTimeoutMs || 30_000,
    connect: {
      timeout: 5_000,
      keepAlive: true,
      keepAliveInitialDelay: 1_000,
    },
  })

  const statsByProtocol = new Map<string, AgentStats>()
  agent.on('connectionError', (origin) => {
    statsForOrigin(statsByProtocol, origin).connectErrorCount++
  })
  agent.on('disconnect', (origin, _targets, error) => {
    const stats = statsForOrigin(statsByProtocol, origin)
    // A socket closed or reset while idle fails no request.
    if (!requestErrors.has(error)) return
    if (
      error.code === 'UND_ERR_HEADERS_TIMEOUT' ||
      error.code === 'UND_ERR_BODY_TIMEOUT' ||
      error.code === 'ETIMEDOUT'
    ) {
      stats.timeoutSocketCount++
    } else if (!['UND_ERR_INFO', 'UND_ERR_ABORTED', 'UND_ERR_DESTROYED'].includes(error.code)) {
      stats.errorSocketCount++
    }
  })

  let watcher: NodeJS.Timeout | undefined
  let closing: Promise<void> | undefined

  return {
    // undici closes the connection after every HEAD unless `reset: false`
    dispatcher: agent.compose((dispatch) => (opts, handler) => {
      if (opts.method === 'HEAD') opts = { ...opts, reset: false }
      let controller: Dispatcher.DispatchController | undefined
      let upload: ReturnType<typeof withUploadTimeout> | undefined
      let sourceError: unknown
      let responseStarted = false
      if (requestTimeoutMs > 0) {
        // Smithy serializes XML requests such as DeleteObjects as strings.
        if (typeof opts.body === 'string') opts = { ...opts, body: Buffer.from(opts.body) }
        if (opts.body instanceof Uint8Array && opts.body.byteLength > MAX_WRITE_CHUNK) {
          opts = { ...opts, body: slices(opts.body) as unknown as Readable }
        }
        // Undici's response timers do not cover an upload waiting for more input.
        if (opts.body instanceof Readable) {
          const source = opts.body
          // Undici's native stream path aborts on a failed source without waiting for the next read.
          source.on('error', (error) => {
            sourceError = error
            controller?.abort(error)
          })
          source.once('close', () => {
            if (!source.readableEnded && !source.errored) {
              controller?.abort(new errors.RequestAbortedError())
            }
          })
          upload = withUploadTimeout(source, requestTimeoutMs, (error) => {
            statsForOrigin(statsByProtocol, String(opts.origin)).timeoutSocketCount++
            // Readable.from() can wait for a stalled generator before emitting its destroy error.
            controller?.abort(error)
          })
          // Undici 7 accepts async iterables, but DispatchOptions.body omits that type.
          opts = { ...opts, body: upload.body as unknown as Readable }
        }
      }
      // compose supplies modern callbacks, including for legacy request handlers.
      const originalHandler = handler
      return dispatch(opts, {
        onRequestStart(requestController, context) {
          controller = requestController
          originalHandler.onRequestStart?.(requestController, context)
        },
        onRequestUpgrade: originalHandler.onRequestUpgrade?.bind(originalHandler),
        onResponseStart(responseController, statusCode, headers, statusMessage) {
          if (statusCode >= 200) {
            responseStarted = true
            upload?.responded()
          }
          originalHandler.onResponseStart?.(responseController, statusCode, headers, statusMessage)
        },
        onResponseData: originalHandler.onResponseData?.bind(originalHandler),
        onResponseEnd(responseController, trailers) {
          upload?.stop()
          originalHandler.onResponseEnd?.(responseController, trailers)
        },
        onResponseError(responseController, error) {
          const code = (error as { code?: string }).code
          if (
            code === 'UND_ERR_BODY_TIMEOUT' ||
            code === 'UND_ERR_SOCKET' ||
            code === 'UND_ERR_RES_CONTENT_LENGTH_MISMATCH'
          ) {
            // Match Node's public names; ECONNRESET keeps XML body failures retryable.
            // Retain Undici's original error and code for diagnostics and pool metrics.
            error = Object.assign(new Error(error.message, { cause: error }), {
              name: responseStarted ? 'Error' : 'TimeoutError',
              code: 'ECONNRESET',
            })
          } else if (
            error !== sourceError &&
            !responseStarted &&
            ['ECONNRESET', 'EPIPE', 'ETIMEDOUT'].includes(code ?? '')
          ) {
            error.name = 'TimeoutError'
          }
          upload?.stop(error)
          originalHandler.onResponseError?.(responseController, error)
        },
      })
    }),
    monitor: () => {
      watcher ??= watchAgent(name, agent, statsByProtocol)
      return watcher
    },
    close: () => {
      clearInterval(watcher)
      closing ??= agent.close()
      return closing
    },
  }
}

function withUploadTimeout(
  body: Readable,
  timeoutMs: number,
  onTimeout: (error: Error) => void
): {
  body: AsyncGenerator<unknown>
  stop: (error?: Error) => void
  responded: () => void
} {
  let timer: NodeJS.Timeout | undefined
  let waitingForInput = false
  let stopped = false
  let responded = false

  function stop(error?: Error) {
    stopped = true
    clearTimeout(timer)
    if (!body.readableEnded) body.destroy(error)
  }

  function onIdle() {
    // A timer may fire during socket drainage; only source input is timed here.
    if (!waitingForInput || responded) return
    // Undici reports the resulting socket close as an informational abort.
    const error = Object.assign(
      new Error(`Upload stream timed out after ${timeoutMs} ms of inactivity`),
      { name: 'TimeoutError', code: 'ETIMEDOUT' }
    )
    body.destroy(error)
    onTimeout(error)
  }

  async function* chunks() {
    const iterator = body[Symbol.asyncIterator]()
    try {
      while (!stopped) {
        waitingForInput = true
        if (timer) timer.refresh()
        else {
          timer = setTimeout(onIdle, timeoutMs)
          timer.unref()
        }
        let result: IteratorResult<unknown>
        try {
          result = await iterator.next()
        } catch {
          // Undici's native stream path reports a source destroyed without an error as aborted.
          throw body.errored ?? new errors.RequestAbortedError()
        } finally {
          waitingForInput = false
        }
        if (result.done || stopped) return
        // Undici asks for the next chunk after draining this one to the socket.
        if (result.value instanceof Uint8Array) yield* slices(result.value)
        else yield result.value
      }
    } finally {
      clearTimeout(timer)
      await iterator.return?.()
    }
  }
  return {
    body: chunks(),
    stop,
    responded: () => {
      responded = true
    },
  }
}

// Undici refreshes headersTimeout only on backpressured writes; one large write can be killed mid-flush.
function* slices(body: Uint8Array) {
  for (let offset = 0; offset < body.byteLength; offset += MAX_WRITE_CHUNK) {
    yield body.subarray(offset, offset + MAX_WRITE_CHUNK)
  }
}

function updateHttpAgentMetrics(name: string, protocol: string, stats: AgentStats) {
  const baseAttrs = { name, protocol }

  httpPoolBusySockets.record(stats.busySocketCount, baseAttrs)
  httpPoolFreeSockets.record(stats.freeSocketCount, baseAttrs)
  httpPoolErrors.record(stats.errorSocketCount, { ...baseAttrs, type: 'socket_error' })
  httpPoolErrors.record(stats.timeoutSocketCount, { ...baseAttrs, type: 'timeout_socket_error' })
  httpPoolErrors.record(stats.connectErrorCount, { ...baseAttrs, type: 'connect_error' })
}

function watchAgent(name: string, dispatcher: Agent, statsByProtocol: Map<string, AgentStats>) {
  const timer = setInterval(() => {
    for (const stats of statsByProtocol.values()) {
      stats.busySocketCount = 0
      stats.freeSocketCount = 0
      stats.pendingRequestCount = 0
    }
    for (const [origin, s] of Object.entries(dispatcher.stats)) {
      const stats = statsForOrigin(statsByProtocol, origin)
      stats.busySocketCount += s.running
      stats.freeSocketCount += 'free' in s ? s.free : Number(s.connected && s.running === 0)
      stats.pendingRequestCount += s.pending
    }
    let pendingRequestCount = 0
    for (const [protocol, stats] of statsByProtocol) {
      updateHttpAgentMetrics(name, protocol, stats)
      pendingRequestCount += stats.pendingRequestCount
    }
    httpPoolPendingRequests.record(pendingRequestCount, { name, region })
  }, 5000)
  timer.unref()
  return timer
}

function statsForOrigin(
  statsByProtocol: Map<string, AgentStats>,
  origin: string | URL
): AgentStats {
  const protocol = new URL(origin).protocol.slice(0, -1)
  let stats = statsByProtocol.get(protocol)
  if (!stats) {
    stats = {
      busySocketCount: 0,
      freeSocketCount: 0,
      pendingRequestCount: 0,
      errorSocketCount: 0,
      timeoutSocketCount: 0,
      connectErrorCount: 0,
    }
    statsByProtocol.set(protocol, stats)
  }
  return stats
}
