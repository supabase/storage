import { FastifyInstance, RequestGenericInterface } from 'fastify'
interface requestGeneric extends RequestGenericInterface {
  Params: {
    objectId: string
  }
}

export default async function routes(fastify: FastifyInstance) {
  fastify.get<requestGeneric>('/:objectId', async (request, reply) => {
    // @todo
    return `metadata ${request.params.objectId}`
  })
  fastify.delete('/:objectId', async (request, reply) => {
    // @todo
  })
  fastify.post('/:objectId', async (request, reply) => {
    // @todo
  })
}
