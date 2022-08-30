import fastifyPlugin from 'fastify-plugin'

interface RequestLoggerOptions {
  excludeUrls?: string[]
}

export default (options: RequestLoggerOptions) =>
  fastifyPlugin(async (fastify) => {
    fastify.addHook('onRequest', async (req) => {
      if (options.excludeUrls?.includes(req.url)) {
        return
      }

      req.log.info({ req }, 'incoming request')
    })

    fastify.addHook('onResponse', async (req, reply) => {
      if (options.excludeUrls?.includes(req.url)) {
        return
      }
      req.log.info({ res: reply }, 'request completed')
    })
  })
