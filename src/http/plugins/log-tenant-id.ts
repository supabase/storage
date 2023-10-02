import fastifyPlugin from 'fastify-plugin'

export const logTenantId = fastifyPlugin(async (fastify) => {
  fastify.addHook('onRequest', async (request, reply) => {
    reply.log = request.log = request.log.child({
      tenantId: request.tenantId,
      project: request.tenantId,
      reqId: request.id,
    })
  })
})
