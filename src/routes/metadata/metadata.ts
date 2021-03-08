import { FastifyInstance, RequestGenericInterface } from 'fastify'
interface requestGeneric extends RequestGenericInterface {
  Params: {
    objectId: string
  }
}

// @todo prevent editing of system metadata like contenttype
// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
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
