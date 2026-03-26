import { multitenantKnex } from '@internal/database'
import {
  isDBMigrationName,
  resetMigrationsOnTenants,
  runMigrationsOnAllTenants,
} from '@internal/database/migrations'
import { PG_BOSS_SCHEMA, Queue } from '@internal/queue'
import { RunMigrationsOnTenants } from '@storage/events'
import { FastifyInstance } from 'fastify'
import { getConfig } from '../../../config'
import apiKey from '../../plugins/apikey'

const { pgQueueEnable } = getConfig()
const migrationQueueName = RunMigrationsOnTenants.getQueueName()

type ResetFleetBody = {
  untilMigration?: unknown
  markCompletedTillMigration?: unknown
}

type FailedQuery = {
  cursor?: string
}

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

    const { untilMigration, markCompletedTillMigration } = req.body as ResetFleetBody

    if (!isDBMigrationName(untilMigration)) {
      return reply.status(400).send({ message: 'Invalid migration' })
    }

    if (
      typeof markCompletedTillMigration === 'string' &&
      !isDBMigrationName(markCompletedTillMigration)
    ) {
      return reply.status(400).send({ message: 'Invalid migration' })
    }

    await resetMigrationsOnTenants({
      till: untilMigration,
      markCompletedTillMigration: isDBMigrationName(markCompletedTillMigration)
        ? markCompletedTillMigration
        : undefined,
      signal: req.signals.disconnect.signal,
    })

    return reply.send({ message: 'Migrations scheduled' })
  })

  fastify.get('/active', async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.code(400).send({ message: 'Queue is not enabled' })
    }
    const data = await multitenantKnex
      .table(`${PG_BOSS_SCHEMA}.job`)
      .where('state', 'active')
      .where('name', migrationQueueName)
      .orderBy('created_on', 'desc')
      .limit(2000)

    return reply.send(data)
  })

  fastify.delete('/active', async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.code(400).send({ message: 'Queue is not enabled' })
    }
    const data = await multitenantKnex
      .table(`${PG_BOSS_SCHEMA}.job`)
      .where('state', 'active')
      .where('name', migrationQueueName)
      .orderBy('created_on', 'desc')
      .update({ state: 'completed' })
      .limit(2000)

    return reply.send(data)
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
    const { cursor } = req.query as FailedQuery
    const offset = cursor ? Number(cursor) : 0

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
