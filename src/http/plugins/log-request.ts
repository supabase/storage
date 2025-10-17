import { PartialHttpData, parsePartialHttp } from '@internal/http'
import { logger, logSchema, redactQueryParamFromRequest } from '@internal/monitoring'
import { FastifyInstance } from 'fastify'
import { FastifyReply } from 'fastify/types/reply'
import { FastifyRequest } from 'fastify/types/request'
import fastifyPlugin from 'fastify-plugin'
import { Socket } from 'net'
import { getConfig } from '../../config'

interface RequestLoggerOptions {
  excludeUrls?: string[]
}

declare module 'fastify' {
  interface FastifyRequest {
    executionError?: Error
    operation?: { type: string }
    resources?: string[]
    startTime: number
    executionTime?: number
  }

  interface FastifyContextConfig {
    operation?: { type: string }
    resources?: (req: FastifyRequest<any>) => string[]
    logMetadata?: (req: FastifyRequest<any>) => Record<string, unknown>
  }
}

const { version } = getConfig()

/**
 * Request logger plugin
 * @param options
 */
export const logRequest = (options: RequestLoggerOptions) =>
  fastifyPlugin(
    async (fastify) => {
      // Used to track cleanup functions per socket
      const socketCleanupMap = new WeakMap<Socket, () => void>()
      const cleanupSocketListeners = (socket: Socket) => {
        const cleanup = socketCleanupMap.get(socket)
        if (cleanup) {
          socketCleanupMap.delete(socket)
          cleanup()
        }
      }

      // Watch for connections that timeout or disconnect before complete HTTP headers are received
      // For keep-alive connections, track each potential request independently
      const onConnection = (socket: Socket) => {
        const captureByteLimit = 2048
        let currentRequestData: Buffer[] = []
        let currentRequestDataSize = 0
        let currentRequestStart = Date.now()
        let waitingForRequest = false
        let pendingRequestLogged = false

        // Store cleanup function so hooks can access it
        socketCleanupMap.set(socket, () => {
          pendingRequestLogged = true
          waitingForRequest = false
          currentRequestData = []
          currentRequestDataSize = 0
        })

        // Capture partial data sent before connection closes
        const onData = (chunk: Buffer) => {
          // Start tracking a new potential request when we receive data after a completed one
          if (!waitingForRequest) {
            waitingForRequest = true
            currentRequestData = []
            currentRequestDataSize = 0
            currentRequestStart = Date.now()
            pendingRequestLogged = false
          }

          const remaining = captureByteLimit - currentRequestDataSize
          if (remaining > 0) {
            const slicedChunk = chunk.subarray(0, Math.min(chunk.length, remaining))
            currentRequestData.push(slicedChunk)
            currentRequestDataSize += slicedChunk.length
          }
        }
        socket.on('data', onData)

        // Remove data listener on socket error to prevent listener leak
        socket.once('error', () => {
          socket.removeListener('data', onData)
        })

        socket.once('close', () => {
          socket.removeListener('data', onData)
          socketCleanupMap.delete(socket)

          // Only log if we were waiting for a request that was never properly logged
          if (!waitingForRequest || currentRequestData.length === 0 || pendingRequestLogged) {
            return
          }

          const parsedHttp = parsePartialHttp(currentRequestData)
          const req = createPartialLogRequest(fastify, socket, parsedHttp, currentRequestStart)

          doRequestLog(req, {
            excludeUrls: options.excludeUrls,
            statusCode: 'ABORTED CONN',
            responseTime: (Date.now() - req.startTime) / 1000,
          })
        })
      }

      fastify.server.on('connection', onConnection)

      // Clean up on close
      fastify.addHook('onClose', async () => {
        fastify.server.removeListener('connection', onConnection)
      })

      fastify.addHook('onRequest', async (req, res) => {
        req.startTime = Date.now()

        res.raw.once('close', () => {
          if (req.raw.aborted) {
            doRequestLog(req, {
              excludeUrls: options.excludeUrls,
              statusCode: 'ABORTED REQ',
              responseTime: (Date.now() - req.startTime) / 1000,
            })
            cleanupSocketListeners(req.raw.socket)
            return
          }

          if (!res.raw.writableFinished) {
            doRequestLog(req, {
              excludeUrls: options.excludeUrls,
              statusCode: 'ABORTED RES',
              responseTime: (Date.now() - req.startTime) / 1000,
            })
            cleanupSocketListeners(req.raw.socket)
          }
        })
      })

      /**
       * Adds req.resources and req.operation to the request object
       */
      fastify.addHook('preHandler', async (req) => {
        const resourceFromParams = Object.values(req.params || {}).join('/')
        const resources = getFirstDefined<string[]>(
          req.resources,
          req.routeOptions.config.resources?.(req),
          (req.raw as any).resources,
          resourceFromParams ? [resourceFromParams] : ([] as string[])
        )

        if (resources && resources.length > 0) {
          resources.map((resource, index) => {
            if (!resource.startsWith('/')) {
              resources[index] = `/${resource}`
            }
          })
        }

        req.resources = resources
        req.operation = req.routeOptions.config.operation

        if (req.operation && typeof req.opentelemetry === 'function') {
          req.opentelemetry()?.span?.setAttribute('http.operation', req.operation.type)
        }
      })

      fastify.addHook('onSend', async (req, _, payload) => {
        req.executionTime = Date.now() - req.startTime
        return payload
      })

      fastify.addHook('onResponse', async (req, reply) => {
        doRequestLog(req, {
          reply,
          excludeUrls: options.excludeUrls,
          statusCode: reply.statusCode,
          responseTime: reply.elapsedTime,
          executionTime: req.executionTime,
        })

        // Mark request as logged so socket close handler doesn't log it again
        cleanupSocketListeners(req.raw.socket)
      })
    },
    { name: 'log-request' }
  )

interface LogRequestOptions {
  reply?: FastifyReply
  excludeUrls?: string[]
  statusCode: number | 'ABORTED REQ' | 'ABORTED RES' | 'ABORTED CONN'
  responseTime: number
  executionTime?: number
}

function doRequestLog(req: FastifyRequest, options: LogRequestOptions) {
  if (options.excludeUrls?.includes(req.url)) {
    return
  }

  const rMeth = req.method
  const rUrl = redactQueryParamFromRequest(req, [
    'token',
    'X-Amz-Credential',
    'X-Amz-Signature',
    'X-Amz-Security-Token',
  ])
  const uAgent = req.headers['user-agent']
  const rId = req.id
  const cIP = req.ip
  const statusCode = options.statusCode
  const error = (req.raw as any).executionError || req.executionError
  const tenantId = req.tenantId

  let reqMetadata: Record<string, unknown> = {}

  if (req.routeOptions.config.logMetadata) {
    try {
      reqMetadata = req.routeOptions.config.logMetadata(req)

      if (reqMetadata) {
        try {
          if (typeof req.opentelemetry === 'function') {
            req.opentelemetry()?.span?.setAttribute('http.metadata', JSON.stringify(reqMetadata))
          }
        } catch (e) {
          // do nothing
          logSchema.warning(logger, 'Failed to serialize log metadata', {
            type: 'otel',
            error: e,
          })
        }
      }
    } catch (e) {
      logSchema.error(logger, 'Failed to get log metadata', {
        type: 'request',
        error: e,
      })
    }
  }

  const buildLogMessage = `${tenantId} | ${rMeth} | ${statusCode} | ${cIP} | ${rId} | ${rUrl} | ${uAgent}`

  logSchema.request(req.log, buildLogMessage, {
    type: 'request',
    req,
    reqMetadata,
    res: options.reply,
    responseTime: options.responseTime,
    executionTime: options.executionTime,
    error,
    owner: req.owner,
    role: req.jwtPayload?.role,
    resources: req.resources,
    operation: req.operation?.type ?? req.routeOptions.config.operation?.type,
    serverTimes: req.serverTimings,
  })
}

function getFirstDefined<T>(...values: any[]): T | undefined {
  for (const value of values) {
    if (value !== undefined) {
      return value
    }
  }
  return undefined
}

/**
 * Creates a minimal FastifyRequest from partial HTTP data.
 * Used for consistent logging when request parsing fails.
 */
export function createPartialLogRequest(
  fastify: FastifyInstance,
  socket: Socket,
  httpData: PartialHttpData,
  startTime: number
) {
  return {
    method: httpData.method,
    url: httpData.url,
    headers: httpData.headers,
    ip: socket.remoteAddress || 'unknown',
    id: 'no-request',
    log: fastify.log.child({
      tenantId: httpData.tenantId,
      project: httpData.tenantId,
      reqId: 'no-request',
      appVersion: version,
      dataLength: httpData.length,
    }),
    startTime,
    tenantId: httpData.tenantId,
    raw: {},
    routeOptions: { config: {} },
    resources: [],
  } as unknown as FastifyRequest
}
