import { subscribe } from 'node:diagnostics_channel'
import type { IncomingHttpHeaders } from 'node:http'
import type { Socket } from 'node:net'
import { finished, Readable } from 'node:stream'
import { createSecureContext } from 'node:tls'
import {
  httpPoolBusySockets,
  httpPoolErrors,
  httpPoolFreeSockets,
  httpPoolPendingRequests,
} from '@internal/monitoring/metrics'
import { Agent, buildConnector, Client, type Dispatcher, errors, Pool } from 'undici'
import { getConfig } from '../../config'

const { region } = getConfig()

const MAX_WRITE_CHUNK = 256 * 1024
const OWS_END = /[\t ]+$/
const noop = () => {}

// Like Node's http client, hold a tick's writes so undici's per-chunk writes leave in one writev.
function coalesceWrites(socket: Socket) {
  // One 64 KiB body chunk fills the default write buffer, so undici would pause the body after each.
  const { _writableState } = socket as Socket & { _writableState: { highWaterMark: number } }
  _writableState.highWaterMark = MAX_WRITE_CHUNK
  const uncork = socket.uncork
  let deferred = 0
  const flush = () => {
    for (; deferred > 0; deferred--) uncork.call(socket)
  }
  socket.uncork = () => {
    if (deferred++ === 0) process.nextTick(flush)
  }
}

// Node's parser keeps the first value of these when a response repeats them; Smithy would join them.
const SINGLETON_HEADERS = new Set([
  'age',
  'authorization',
  'content-length',
  'content-type',
  'etag',
  'expires',
  'from',
  'host',
  'if-modified-since',
  'if-unmodified-since',
  'last-modified',
  'location',
  'max-forwards',
  'proxy-authorization',
  'referer',
  'retry-after',
  'server',
  'user-agent',
])

type Connection = { socket?: Socket; waiting: Set<Handler>; cancel: () => void }
type Handler = Dispatcher.DispatchHandler & { failed?: Error; connection?: Connection }

// Undici's Pool cannot drop a request that failed while it waited there, and it marks a client busy
// forever when Client#dispatch fails synchronously (e.g. an invalid header value).
function createClient(origin: string | URL, opts: Client.Options) {
  let connecting: Socket | undefined
  const connection: Connection = {
    // After a stale 'drain' undici can queue a second request on a connecting client.
    waiting: new Set(),
    cancel: () => {
      if (connection.waiting.size === 0) connecting?.destroy(new errors.RequestAbortedError())
    },
  }
  const connect = opts.connect as buildConnector.connector
  const client = new Client(origin, {
    ...opts,
    connect(options, callback) {
      // buildConnector returns the socket synchronously, although its types say void.
      connecting = connect(options, (...args) => {
        connecting = undefined
        connection.socket = args[1] ?? undefined
        callback(...args)
      }) as unknown as Socket
    },
  })
  const dispatch = client.dispatch.bind(client)
  client.dispatch = (options, handler: Handler) => {
    if (handler.failed !== undefined) return true
    handler.connection = connection
    connection.waiting.add(handler)
    return dispatch(options, handler) || handler.failed !== undefined
  }
  return client
}

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

export interface AgentStats {
  busySocketCount: number
  freeSocketCount: number
  pendingRequestCount: number
  errorSocketCount: number
  timeoutSocketCount: number
  createSocketErrorCount: number
}

export interface AgentOptions {
  maxSockets: number
  /** Socket acquisition and connection timeout in milliseconds; zero disables it. */
  connectionTimeoutMs?: number
  /** Header, response-body and upload-input timeout in milliseconds; zero idles headers, upload input and bodies out after bodyTimeoutMs (30 s). */
  requestTimeoutMs?: number
  /** Response-header timeout for S3 copy requests; zero disables it. */
  copyTimeoutMs?: number
  /** Response-body idle timeout in milliseconds, and the header and upload-input timeout when requestTimeoutMs is zero; zero disables it. */
  bodyTimeoutMs?: number
}

export function createAgent(name: string, options: AgentOptions): InstrumentedAgent {
  const connectionTimeoutMs = options.connectionTimeoutMs ?? 5_000
  const requestTimeoutMs = options.requestTimeoutMs ?? 0
  const copyTimeoutMs = options.copyTimeoutMs ?? requestTimeoutMs
  const bodyTimeoutMs = options.bodyTimeoutMs ?? (requestTimeoutMs || 30_000)
  // agentkeepalive's 30 s working-socket timeout also bounded silent headers and upload input.
  const idleTimeoutMs = requestTimeoutMs || bodyTimeoutMs
  const connector = buildConnector({
    allowH2: false,
    timeout: connectionTimeoutMs,
    keepAlive: true,
    keepAliveInitialDelay: 1_000,
    // Otherwise tls.connect() builds an identical SecureContext for every connection.
    secureContext: createSecureContext(),
  })
  const agent = new Agent({
    factory: (origin, opts) => new Pool(origin, { ...opts, factory: createClient }),
    allowH2: false,
    connections: options.maxSockets,
    keepAliveTimeout: 15_000,
    keepAliveMaxTimeout: 15_000,
    pipelining: 1,
    // Undici's header timer ignores partial header bytes; Node's socket timeout counts them.
    headersTimeout: 0,
    // Undici's body timer pauses with backpressure; Node's socket timeout did not, so own it.
    bodyTimeout: 0,
    connect: (opts, callback) =>
      connector(opts, (...args) => {
        const socket = args[1]
        if (socket) coalesceWrites(socket)
        callback(...args)
        // Node's TLS socket reports record and alert errors without closing, unlike its http client.
        socket?.on('error', () => socket.destroy())
      }),
  })

  const statsByProtocol = new Map<string, AgentStats>()
  agent.on('connectionError', (origin, _targets, error) => {
    const stats = statsForOrigin(statsByProtocol, origin)
    if (error.code !== 'UND_ERR_ABORTED') stats.createSocketErrorCount++
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
  let active = 0
  let drained: PromiseWithResolvers<void> | undefined
  // Undici frees a socket one event-loop turn after its response ends.
  let reusable = 0
  const releaseReusable = () => {
    reusable--
  }

  return {
    // undici closes the connection after every HEAD unless `reset: false`
    dispatcher: agent.compose((dispatch) => (opts, handler) => {
      active++
      if (opts.method === 'HEAD') opts = { ...opts, reset: false }
      const source = opts.body instanceof Readable ? opts.body : undefined
      let controller: Dispatcher.DispatchController | undefined
      let upload: UploadBody | undefined
      let status = 0
      let settled = false
      let socket: Socket | undefined
      let timer: NodeJS.Timeout | undefined
      let expired = false
      let announced = false
      // Undici cannot abort a request it has not started, so fail the caller.
      const abort = (error?: Error | null) => {
        error ??= new errors.RequestAbortedError()
        if (controller) controller.abort(error)
        else wrapped.onResponseError?.(null as unknown as Dispatcher.DispatchController, error)
      }
      if (source) failOnSource(source, abort)
      // S3-compatible stores can take longer than the default timeout to complete copies.
      const copy = (opts.headers as Record<string, unknown> | undefined)?.['x-amz-copy-source']
      const headersTimeoutMs = opts.headersTimeout ?? (copy ? copyTimeoutMs : idleTimeoutMs)
      if (idleTimeoutMs > 0) {
        // Smithy serializes XML requests such as DeleteObjects as strings.
        if (typeof opts.body === 'string') opts = { ...opts, body: Buffer.from(opts.body) }
        if (
          opts.body instanceof Uint8Array &&
          opts.body.byteLength > MAX_WRITE_CHUNK &&
          // A plain socket's timeout sees progress within a pending write; TLS sees completed writes only.
          !String(opts.origin).startsWith('http:')
        ) {
          // Undici accepts iterables here, but DispatchOptions.body omits that type.
          opts = { ...opts, body: slices(opts.body) as unknown as Readable }
        }
      }
      if (source) {
        upload = withUploadTimeout(source, idleTimeoutMs, (error) => {
          statsForOrigin(statsByProtocol, String(opts.origin)).timeoutSocketCount++
          // Readable.from() can wait for a stalled generator before emitting its destroy error.
          controller?.abort(error)
        })
        opts = { ...opts, body: upload.body }
      }
      const expire = () => {
        if (status) {
          // A paused consumer may have every byte buffered; let undici parse what it holds first.
          controller?.resume()
          if (settled) return
          statsForOrigin(statsByProtocol, String(opts.origin)).timeoutSocketCount++
          controller?.abort(new errors.BodyTimeoutError())
          return
        }
        // Spare a body still waiting for input, which the upload timer owns; unsent bytes mean the peer stalled.
        if (upload && !upload.body.readableEnded && !socket?.writableLength) {
          socket?.setTimeout(headersTimeoutMs)
          return
        }
        statsForOrigin(statsByProtocol, String(opts.origin)).timeoutSocketCount++
        controller?.abort(new errors.HeadersTimeoutError())
      }
      const watch = (s: Socket) => {
        socket = s
        s.on('timeout', expire)
        if (headersTimeoutMs > 0) s.setTimeout(headersTimeoutMs)
      }
      const unwatch = () => {
        socket?.setTimeout(0).off('timeout', expire)
        socket = undefined
      }
      const settle = () => {
        if (settled) return
        settled = true
        if (--active === 0 && drained) {
          drained.resolve()
          drained = undefined
        }
      }
      const wrapped: Handler = {
        onRequestStart(requestController, context) {
          // Throwing skips the request and keeps the socket; aborting here would destroy it.
          if (wrapped.failed !== undefined) throw wrapped.failed
          wrapped.connection?.waiting.delete(wrapped)
          controller = requestController
          clearTimeout(timer)
          // Undici writes the request on this socket right after this call.
          const connected = wrapped.connection?.socket
          if (connected) watch(connected)
          if (!announced) handler.onRequestStart?.(requestController, context)
        },
        onRequestUpgrade(responseController, statusCode, headers, socket) {
          unwatch()
          try {
            handler.onRequestUpgrade?.(responseController, statusCode, headers, socket)
          } finally {
            settle()
          }
        },
        onResponseStart(responseController, statusCode, headers, statusMessage) {
          if (statusCode >= 200) {
            status = statusCode
            upload?.responded()
            if (
              bodyTimeoutMs > 0 &&
              opts.method !== 'HEAD' &&
              statusCode !== 204 &&
              statusCode !== 304 &&
              headers['content-length'] !== '0'
            ) {
              socket?.setTimeout(bodyTimeoutMs)
            } else {
              unwatch()
            }
          }
          handler.onResponseStart?.(
            responseController,
            statusCode,
            nodeHeaders(headers),
            statusMessage
          )
          // Like Node's IncomingMessage, a body destroyed before its end must not raise an unhandled error.
          const body = (handler as { res?: unknown }).res
          if (body instanceof Readable) body.on('error', noop)
        },
        onResponseData(responseController, chunk) {
          // TLS can deliver buffered plaintext without a socket read; payload progress counts too.
          if (bodyTimeoutMs > 0) socket?.setTimeout(bodyTimeoutMs)
          handler.onResponseData?.(responseController, chunk)
        },
        onResponseEnd(responseController, trailers) {
          unwatch()
          // S3 can accept a body before the caller's source emits 'end'; let the source finish.
          // After an error response the caller still owns its source: destroying it would abort
          // a caller's pipeline before the SDK parses the S3 error.
          if (status < 300) upload?.finish()
          else upload?.detach()
          reusable++
          setImmediate(releaseReusable)
          try {
            handler.onResponseEnd?.(responseController, trailers)
          } finally {
            settle()
          }
        },
        onResponseError(responseController, error) {
          if (wrapped.failed !== undefined) return
          wrapped.failed = error
          wrapped.connection?.waiting.delete(wrapped)
          clearTimeout(timer)
          // Like Node's agent, stop connecting for a request cancelled while it waited. An expired
          // one is left to the connect timeout, which reports it as a connection error.
          if (!expired) wrapped.connection?.cancel()
          unwatch()
          const code = (error as { code?: string }).code
          if (error === (opts as { signal?: AbortSignal }).signal?.reason) {
            // The SDK retries TimeoutError names and callers classify aborts by the AbortError name.
            error = Object.assign(new Error('Request aborted', { cause: error }), {
              name: 'AbortError',
            })
          } else if (
            code === 'UND_ERR_BODY_TIMEOUT' ||
            code === 'UND_ERR_SOCKET' ||
            // Before the final headers this is a parse error, which Node does not retry either.
            (code === 'UND_ERR_RES_CONTENT_LENGTH_MISMATCH' && status > 0) ||
            (status > 0 &&
              (code?.startsWith('ERR_SSL_') ||
                code === 'UND_ERR_HEADERS_OVERFLOW' ||
                error instanceof errors.HTTPParserError))
          ) {
            // Match Node's public names; ECONNRESET keeps XML body failures retryable.
            error = Object.assign(new Error(error.message, { cause: error }), {
              name: status ? 'Error' : 'TimeoutError',
              code: 'ECONNRESET',
            })
          } else if (
            error !== source?.errored &&
            !status &&
            ['ECONNRESET', 'EPIPE', 'ETIMEDOUT'].includes(code ?? '')
          ) {
            error.name = 'TimeoutError'
          } else if (code === 'UND_ERR_CONNECT_TIMEOUT') {
            // Undici names the S3 address, which storage would echo in API error bodies.
            error.message = 'Connect Timeout Error'
          }
          try {
            upload?.stop(error)
            handler.onResponseError?.(responseController, error)
          } finally {
            settle()
          }
        },
      }
      const send = () => {
        let free: boolean
        try {
          free = dispatch(opts, wrapped)
        } catch (error) {
          if (settled) throw error
          wrapped.onResponseError?.(
            controller ?? (null as unknown as Dispatcher.DispatchController),
            error instanceof Error ? error : new Error(String(error), { cause: error })
          )
          return false
        }
        if (controller || wrapped.failed !== undefined) return free
        // Match NodeHttpHandler's deadline across pool waiting and connection establishment.
        if (connectionTimeoutMs > 0) {
          timer = setTimeout(() => {
            expired = true
            abort(
              Object.assign(
                new Error(
                  `Request socket did not become available within ${connectionTimeoutMs} ms`
                ),
                { code: 'ETIMEDOUT' }
              )
            )
          }, connectionTimeoutMs)
        }
        // Undici hands out its controller at the start; the caller must be able to abort before.
        announced = true
        handler.onRequestStart?.(
          {
            get aborted() {
              return wrapped.failed !== undefined || controller?.aborted === true
            },
            get reason() {
              return wrapped.failed ?? controller?.reason ?? null
            },
            get paused() {
              return controller?.paused ?? false
            },
            pause: () => controller?.pause(),
            resume: () => controller?.resume(),
            abort,
          },
          {}
        )
        return free
      }
      if (reusable > 0) {
        setImmediate(send)
        return true
      }
      return send()
    }),
    monitor: () => {
      watcher ??= watchAgent(name, agent, statsByProtocol)
      return watcher
    },
    close: () => {
      if (watcher) {
        clearInterval(watcher)
        watcher = undefined
      }
      // Handlers whose clients left still clean up, so keep serving; destroy() tears down.
      if (active === 0) return Promise.resolve()
      drained ??= Promise.withResolvers<void>()
      return drained.promise
    },
  }
}

interface UploadBody {
  body: Readable
  stop: (error?: Error) => void
  responded: () => void
  finish: () => void
  detach: () => void
}

// Undici's native stream path aborts on a failed source without waiting for the next read.
function failOnSource(source: Readable, fail: (error: Error) => void) {
  finished(source, { writable: false }, (error) => {
    if (error) fail(source.errored ?? new errors.RequestAbortedError())
  })
}

// Match Node's parser: trim trailing spaces and tabs.
function nodeHeaders(headers: IncomingHttpHeaders) {
  for (const name in headers) {
    const value = headers[name]
    if (Array.isArray(value)) {
      headers[name] = SINGLETON_HEADERS.has(name) ? trimHeader(value[0]) : value.map(trimHeader)
    } else if (value) {
      headers[name] = trimHeader(value)
    }
  }
  return headers
}

function trimHeader(value: string) {
  const last = value.charCodeAt(value.length - 1)
  return last === 32 || last === 9 ? value.replace(OWS_END, '') : value
}

// Undici destroys a stream body it owns when the response ends first; the proxy lets a source
// that S3 already accepted still reach its end. Undici's response timers do not cover an upload
// waiting for more input, so the proxy also times that out unless timeoutMs is 0.
function withUploadTimeout(
  source: Readable,
  timeoutMs: number,
  onTimeout: (error: Error) => void
): UploadBody {
  let timer: NodeJS.Timeout | undefined
  let waitingForInput = false
  let responded = false
  let succeeded = false
  let detached = false
  let started = false
  let piece: Buffer | undefined
  let offset = 0

  // Object mode preserves each chunk (including aws-chunked framing) without
  // Readable's iterator concatenating buffers or allocating promises per chunk.
  const body = new Readable({
    objectMode: true,
    // One aws-chunked frame consists of a size line, payload and CRLF.
    highWaterMark: 3,
    read() {
      if (!started) {
        started = true
        source.on('data', onData)
        source.once('end', onEnd)
      }
      pump()
    },
  }).on('error', () => {})

  function waitForInput() {
    waitingForInput = true
    if (responded || timeoutMs === 0) return
    if (timer) timer.refresh()
    else {
      timer = setTimeout(() => {
        // A paused consumer is waiting for socket drainage, not upload input.
        if (!waitingForInput || body.isPaused() || responded) return
        onTimeout(
          Object.assign(new Error(`Upload stream timed out after ${timeoutMs} ms of inactivity`), {
            name: 'TimeoutError',
            code: 'ETIMEDOUT',
          })
        )
      }, timeoutMs)
      timer.unref()
    }
  }

  function pump() {
    if (body.destroyed) return
    while (piece) {
      const chunk =
        offset === 0 && piece.length <= MAX_WRITE_CHUNK
          ? piece
          : piece.subarray(offset, offset + MAX_WRITE_CHUNK)
      offset += chunk.length
      if (offset === piece.length) piece = undefined
      if (!body.push(chunk)) {
        source.pause()
        return
      }
    }
    if (source.readableEnded) {
      body.push(null)
    } else {
      waitForInput()
      source.resume()
    }
  }

  function onData(chunk: Buffer | string) {
    waitingForInput = false
    piece = typeof chunk === 'string' ? Buffer.from(chunk) : chunk
    offset = 0
    if (piece.length === 0) piece = undefined
    pump()
  }

  function onEnd() {
    clearTimeout(timer)
    if (!piece) body.push(null)
  }

  body.on('resume', () => {
    if (waitingForInput) waitForInput()
  })
  body.once('close', () => {
    clearTimeout(timer)
    piece = undefined
    source.off('data', onData)
    source.off('end', onEnd)
    if (!source.readableEnded) {
      if (succeeded) source.resume()
      else if (detached) source.pause()
      else source.destroy()
    }
  })

  return {
    body,
    stop: (error) => {
      // An error on the body would make undici destroy the socket when it starts the request.
      body.destroy()
      if (!source.readableEnded) source.destroy(error)
    },
    responded: () => {
      responded = true
      clearTimeout(timer)
    },
    finish: () => {
      succeeded = true
    },
    detach: () => {
      detached = true
      body.destroy()
    },
  }
}

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
  httpPoolErrors.record(stats.createSocketErrorCount, { ...baseAttrs, type: 'create_socket_error' })
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
      createSocketErrorCount: 0,
    }
    statsByProtocol.set(protocol, stats)
  }
  return stats
}
