import fastifyPlugin from 'fastify-plugin'
import { redactQueryParamFromRequest } from '../monitoring'

interface RequestLoggerOptions {
  excludeUrls?: string[]
}

export default (options: RequestLoggerOptions) =>
  fastifyPlugin(async (fastify) => {
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

      const buildLogMessage = `${rMeth} | ${statusCode} | ${cIP} | ${rId} | ${rUrl} | ${uAgent}`

      req.log.info({ req, res: reply, responseTime: reply.getResponseTime() }, buildLogMessage)
    })
  })
