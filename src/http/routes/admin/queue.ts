import { FastifyInstance } from 'fastify'
import apiKey from '../../plugins/apikey'
import { getConfig } from '../../../config'
import { UpgradePgBossV10 } from '@storage/events'

const { pgQueueEnable } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.post('/migrate/pgboss-v10', async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.status(400).send({ message: 'Queue is not enabled' })
    }

    await UpgradePgBossV10.send({})

    return reply.send({ message: 'Migration scheduled' })
  })
}
