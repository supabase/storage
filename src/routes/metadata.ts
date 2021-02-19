import { FastifyInstance, RequestGenericInterface } from 'fastify'
interface requestGeneric extends RequestGenericInterface {
  Params: {
    objectId: string
  }
}

export default async function routes(fastify: FastifyInstance) {
  fastify.get<requestGeneric>('/metadata/:objectId', async (request, reply) => {
    return `metadata ${request.params.objectId}`
  })
  fastify.delete('/metadata/:objectId', async (request, reply) => {})
  fastify.post('/metadata/:objectId', async (request, reply) => {})
}
