import { logSchema, redactQueryParamFromRequest } from '@internal/monitoring'
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
      fastify.addHook('onRequest', async (req, res) => {
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
      })

      /**
       * Adds req.resources and req.operation to the request object
       */
      fastify.addHook('preHandler', async (req) => {
        let resources = req.resources

        if (resources === undefined) {
          resources = req.routeOptions.config.resources?.(req)
        }

        if (resources === undefined) {
          resources = req.raw.resources
        }

        if (resources === undefined) {
          const params = req.params as Record<string, unknown> | undefined
          let resourceFromParams = ''

          if (params) {
            let first = true
            for (const key in params) {
              if (!Object.prototype.hasOwnProperty.call(params, key)) {
                continue
              }

              if (!first) {
                resourceFromParams += '/'
              }

              const value = params[key]
              resourceFromParams += value == null ? '' : String(value)
              first = false
            }
          }

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
  const error = req.raw.executionError || req.executionError
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
