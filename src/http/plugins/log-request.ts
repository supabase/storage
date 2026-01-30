import fastifyPlugin from 'fastify-plugin'
import { logger, logSchema, redactQueryParamFromRequest } from '@internal/monitoring'
import { trace } from '@opentelemetry/api'
import { FastifyRequest } from 'fastify/types/request'
import { FastifyReply } from 'fastify/types/reply'

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

        if (req.operation) {
          trace.getActiveSpan()?.setAttribute('http.operation', req.operation.type)
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
  const error = (req.raw as any).executionError || req.executionError
  const tenantId = req.tenantId

  let reqMetadata: Record<string, unknown> = {}

  if (req.routeOptions.config.logMetadata) {
    try {
      reqMetadata = req.routeOptions.config.logMetadata(req)

      if (reqMetadata) {
        try {
          trace.getActiveSpan()?.setAttribute('http.metadata', JSON.stringify(reqMetadata))
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
    error: error,
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
