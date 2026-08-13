import {
  isDBMigrationName,
  resetMigrationsOnTenants,
  runMigrationsOnAllTenants,
} from '@internal/database/migrations'
import { FastifyInstance } from 'fastify'
import { getConfig } from '../../../config'
import { registerApiKeyAuth } from '../../plugins/apikey'

const { pgQueueEnable } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  registerApiKeyAuth(fastify)

  fastify.post('/migrate/fleet', { schema: { tags: ['migration'] } }, async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.status(400).send({ message: 'Queue is not enabled' })
    }

    await runMigrationsOnAllTenants({
      signal: req.signals.disconnect.signal,
      sbReqId: req.sbReqId,
    })

    return reply.send({ message: 'Migrations scheduled' })
  })

  fastify.post('/reset/fleet', { schema: { tags: ['migration'] } }, async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.status(400).send({ message: 'Queue is not enabled' })
    }

    const { untilMigration, markCompletedTillMigration } = req.body as Record<string, unknown>

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
      sbReqId: req.sbReqId,
    })

    return reply.send({ message: 'Migrations scheduled' })
  })
}
