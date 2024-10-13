import fastifyPlugin from 'fastify-plugin'
import { logSchema, redactQueryParamFromRequest } from '@internal/monitoring'
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
  }

  interface FastifyContextConfig {
    operation?: { type: string }
    resources?: (req: FastifyRequest<any>) => string[]
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
          req.routeConfig.resources?.(req),
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
        req.operation = req.routeConfig.operation

        if (req.operation) {
          trace.getActiveSpan()?.setAttribute('http.operation', req.operation.type)
        }
      })

      fastify.addHook('onResponse', async (req, reply) => {
        doRequestLog(req, {
          reply,
          excludeUrls: options.excludeUrls,
          statusCode: reply.statusCode,
          responseTime: reply.elapsedTime,
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

  const buildLogMessage = `${tenantId} | ${rMeth} | ${statusCode} | ${cIP} | ${rId} | ${rUrl} | ${uAgent}`

  logSchema.request(req.log, buildLogMessage, {
    type: 'request',
    req,
    res: options.reply,
    responseTime: options.responseTime,
    error: error,
    owner: req.owner,
    role: req.jwtPayload?.role,
    resources: req.resources,
    operation: req.operation?.type ?? req.routeConfig.operation?.type,
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
