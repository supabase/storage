import { FastifyInstance } from 'fastify'
import apiKey from '../../plugins/apikey'
import { Queue, RunMigrationsOnTenants } from '../../../queue'
import { getConfig } from '../../../config'
import { multitenantKnex } from '../../../database'

const { pgQueueEnable } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.get('/progress', async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.code(400).send({ message: 'Queue is not enabled' })
    }
    const queueSize = await Queue.getInstance().getQueueSize(RunMigrationsOnTenants.getQueueName())
    return { remaining: queueSize }
  })

  fastify.get('/failed', async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.code(400).send({ message: 'Queue is not enabled' })
    }
    const offset = (req.query as any).cursor ? Number((req.query as any).cursor) : 0

    const failed = await multitenantKnex
      .table('tenants')
      .where('migrations_status', 'FAILED')
      .where('cursor_id', '>', offset)
      .limit(50)
      .select('id', 'cursor_id')
      .orderBy('cursor_id')

    reply.status(200).send({
      next_cursor_id: failed[failed.length - 1]?.cursor_id || null,
      data: failed,
    })
  })
}
