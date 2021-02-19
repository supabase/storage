import { FastifyInstance } from 'fastify'

export default async function routes(fastify: FastifyInstance) {
  fastify.get('/bucket/:bucketId', async (request, reply) => {})
  fastify.delete('/bucket/:bucketId', async (request, reply) => {})
  fastify.post('/bucket/:bucketId', async (request, reply) => {})
}
