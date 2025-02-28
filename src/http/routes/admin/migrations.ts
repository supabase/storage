import { FastifyInstance } from 'fastify'
import { Queue } from '@internal/queue'
import { multitenantKnex } from '@internal/database'
import { RunMigrationsOnTenants } from '@storage/events'
import apiKey from '../../plugins/apikey'
import { getConfig } from '../../../config'
import {
  DBMigration,
  resetMigrationsOnTenants,
  runMigrationsOnAllTenants,
} from '@internal/database/migrations'

const { pgQueueEnable } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.post('/migrate/fleet', async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.status(400).send({ message: 'Queue is not enabled' })
    }

    await runMigrationsOnAllTenants(req.signals.disconnect.signal)

    return reply.send({ message: 'Migrations scheduled' })
  })

  fastify.post('/reset/fleet', async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.status(400).send({ message: 'Queue is not enabled' })
    }

    const { untilMigration, markCompletedTillMigration } = req.body as any

    if (
      typeof untilMigration !== 'string' ||
      !DBMigration[untilMigration as keyof typeof DBMigration]
    ) {
      return reply.status(400).send({ message: 'Invalid migration' })
    }

    if (
      typeof markCompletedTillMigration === 'string' &&
      !DBMigration[untilMigration as keyof typeof DBMigration]
    ) {
      return reply.status(400).send({ message: 'Invalid migration' })
    }

    await resetMigrationsOnTenants({
      till: untilMigration as keyof typeof DBMigration,
      markCompletedTillMigration: markCompletedTillMigration
        ? markCompletedTillMigration
        : undefined,
      signal: req.signals.disconnect.signal,
    })

    return reply.send({ message: 'Migrations scheduled' })
  })

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
