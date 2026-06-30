import { logSchema, serializeReplyLog, serializeRequestLog } from '@internal/monitoring'
import type { FastifyReply, FastifyRequest } from 'fastify'
import fastifyPlugin from 'fastify-plugin'

interface RequestLoggerOptions {
  excludeUrls?: string[]
}

type BivariantHandler<Args extends unknown[], Return> = {
  bivarianceHack(...args: Args): Return
}['bivarianceHack']

declare module 'http' {
  interface IncomingMessage {
    executionError?: Error
    resources?: string[]
  }
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
    resources?: BivariantHandler<[req: FastifyRequest], string[]>
    logMetadata?: BivariantHandler<[req: FastifyRequest], Record<string, unknown>>
  }
}

/**
 * Request logger plugin
 * @param options
 */
export const logRequest = (options: RequestLoggerOptions) =>
  fastifyPlugin(
    async (fastify) => {
      fastify.addHook('onRequest', (req, res, done) => {
        req.startTime = Date.now()

        res.raw.once('close', () => {
          if (req.raw.aborted) {
            doRequestLog(req, {
              excludeUrls: options.excludeUrls,
              statusCode: 'ABORTED REQ',
              responseTime: (Date.now() - req.startTime) / 1000,
            })
            return
          }

          if (!res.raw.writableFinished) {
            doRequestLog(req, {
              excludeUrls: options.excludeUrls,
              statusCode: 'ABORTED RES',
              responseTime: (Date.now() - req.startTime) / 1000,
            })
          }
        })
        done()
      })

      /**
       * Adds req.resources and req.operation to the request object
       */
      fastify.addHook('preHandler', (req, _reply, done) => {
        let resources = req.resources

        if (resources === undefined) {
          resources = req.routeOptions.config.resources?.(req)
        }

        if (resources === undefined) {
          resources = req.raw.resources
        }

        if (resources === undefined) {
          const resourceFromParams = getResourceFromParams(req.params)
          resources = resourceFromParams ? [resourceFromParams] : []
        }

        if (resources && resources.length > 0) {
          for (let index = 0; index < resources.length; index++) {
            const resource = resources[index]
            if (!resource.startsWith('/')) {
              resources[index] = `/${resource}`
            }
          }
        }

        req.resources = resources
        req.operation = req.routeOptions.config.operation

        if (req.operation && typeof req.opentelemetry === 'function') {
          req.opentelemetry()?.span?.setAttribute('http.operation', req.operation.type)
        }
        done()
      })

      fastify.addHook('onSend', (req, _reply, payload, done) => {
        req.executionTime = Date.now() - req.startTime
        done(null, payload)
      })

      fastify.addHook('onResponse', (req, reply, done) => {
        doRequestLog(req, {
          reply,
          excludeUrls: options.excludeUrls,
          statusCode: reply.statusCode,
          responseTime: reply.elapsedTime,
          executionTime: req.executionTime,
        })
        done()
      })
    },
    { name: 'log-request' }
  )

interface LogRequestOptions {
  reply?: FastifyReply
  excludeUrls?: string[]
  statusCode: number | 'ABORTED REQ' | 'ABORTED RES'
  responseTime: number
  executionTime?: number
}

function doRequestLog(req: FastifyRequest, options: LogRequestOptions) {
  if (options.excludeUrls?.includes(req.url)) {
    return
  }

  const requestLog = serializeRequestLog(req)
  const replyLog = serializeReplyLog(options.reply)
  const rMeth = requestLog.method
  const rUrl = requestLog.url
  const uAgent = req.headers['user-agent']
  const rId = req.id
  const cIP = req.ip
  const statusCode = options.statusCode
  const error = req.raw.executionError || req.executionError
  const tenantId = req.tenantId

  let reqMetadata = '{}'

  if (req.routeOptions.config.logMetadata) {
    try {
      const metadata = req.routeOptions.config.logMetadata(req)

      if (metadata) {
        try {
          reqMetadata = JSON.stringify(metadata)

          if (typeof req.opentelemetry === 'function') {
            req.opentelemetry()?.span?.setAttribute('http.metadata', reqMetadata)
          }
        } catch (e) {
          // do nothing
          logSchema.warning(req.log, 'Failed to serialize log metadata', {
            type: 'otel',
            tenantId,
            project: tenantId,
            reqId: rId,
            sbReqId: req.sbReqId,
            error: e,
          })
        }
      }
    } catch (e) {
      logSchema.error(req.log, 'Failed to get log metadata', {
        type: 'request',
        tenantId,
        project: tenantId,
        reqId: rId,
        sbReqId: req.sbReqId,
        error: e,
      })
    }
  }

  const buildLogMessage = `${tenantId} | ${rMeth} | ${statusCode} | ${cIP} | ${rId} | ${rUrl} | ${uAgent}`

  logSchema.request(req.log, buildLogMessage, {
    type: 'request',
    tenantId,
    project: tenantId,
    reqId: rId,
    sbReqId: req.sbReqId,
    req: requestLog,
    reqMetadata,
    res: replyLog,
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

function getResourceFromParams(params: unknown): string {
  if (!params || typeof params !== 'object') {
    return ''
  }

  let resource = ''
  let first = true

  for (const key in params) {
    if (!Object.prototype.hasOwnProperty.call(params, key)) {
      continue
    }

    if (!first) {
      resource += '/'
    }

    const value = (params as Record<string, unknown>)[key]
    resource += value == null ? '' : String(value)
    first = false
  }

  return resource
}
