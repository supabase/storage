import fastifyPlugin from 'fastify-plugin'
import { logSchema, redactQueryParamFromRequest } from '../../monitoring'

interface RequestLoggerOptions {
  excludeUrls?: string[]
}

declare module 'fastify' {
  interface FastifyRequest {
    executionError?: Error
  }
}

export const logRequest = (options: RequestLoggerOptions) =>
  fastifyPlugin(async (fastify) => {
    fastify.addHook('onRequestAbort', (req) => {
      if (options.excludeUrls?.includes(req.url)) {
        return
      }

      const rMeth = req.method
      const rUrl = redactQueryParamFromRequest(req, ['token'])
      const uAgent = req.headers['user-agent']
      const rId = req.id
      const cIP = req.ip
      const error = (req.raw as any).executionError || req.executionError
      const tenantId = req.tenantId

      const buildLogMessage = `${tenantId} | ${rMeth} | ABORTED | ${cIP} | ${rId} | ${rUrl} | ${uAgent}`

      logSchema.request(req.log, buildLogMessage, {
        type: 'request',
        req,
        responseTime: 0,
        error: error,
        owner: req.owner,
      })
    })

    fastify.addHook('onResponse', async (req, reply) => {
      if (options.excludeUrls?.includes(req.url)) {
        return
      }

      const rMeth = req.method
      const rUrl = redactQueryParamFromRequest(req, ['token'])
      const uAgent = req.headers['user-agent']
      const rId = req.id
      const cIP = req.ip
      const statusCode = reply.statusCode
      const error = (reply.raw as any).executionError || req.executionError
      const tenantId = req.tenantId

      const buildLogMessage = `${tenantId} | ${rMeth} | ${statusCode} | ${cIP} | ${rId} | ${rUrl} | ${uAgent}`

      logSchema.request(req.log, buildLogMessage, {
        type: 'request',
        req,
        res: reply,
        responseTime: reply.getResponseTime(),
        error: error,
        owner: req.owner,
      })
    })
  })
