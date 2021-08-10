import fastifyPlugin from 'fastify-plugin'

declare module 'fastify' {
  interface FastifyRequest {
    jwt: string
  }
}

export default fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('jwt', null)
  fastify.addHook('preHandler', async (request) => {
    request.jwt = (request.headers.authorization as string).substring('Bearer '.length)
  })
})
