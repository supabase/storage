import { FastifyInstance } from 'fastify'
import apiKey from '../../plugins/apikey'
import { Queue, RunMigrationsOnTenants } from '../../../queue'
import { getConfig } from '../../../config'

const { pgQueueEnable } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.get('/', async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.code(400).send({ message: 'Queue is not enabled' })
    }
    const queueSize = await Queue.getInstance().getQueueSize(RunMigrationsOnTenants.getQueueName())
    return { queueSize }
  })
}
