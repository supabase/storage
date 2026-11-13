import {
  isDBMigrationName,
  resetMigrationsOnTenants,
  runMigrationsOnAllTenants,
} from '@internal/database/migrations'
import { getStorageQueue, QUEUES } from '@storage/events'
import { FastifyInstance } from 'fastify'
import { getConfig } from '../../../config'
import { registerApiKeyAuth } from '../../plugins/apikey'

const { pgQueueEnable } = getConfig()

/** The migration queue's sampled gauges, from the running engine (pg-boss or SQS alike). */
async function migrationQueueStats() {
  const report = await getStorageQueue().stats({ queues: [QUEUES.runMigrations] })
  return report.queues.find((stats) => stats.queue === QUEUES.runMigrations)
}

export default async function routes(fastify: FastifyInstance) {
  registerApiKeyAuth(fastify)

  // Migration jobs running right now. A count only, so it reads the same on every engine.
  fastify.get('/active', { schema: { tags: ['migration'] } }, async (_req, reply) => {
    if (!pgQueueEnable) {
      return reply.code(400).send({ message: 'Queue is not enabled' })
    }
    const stats = await migrationQueueStats()
    return reply.send({ active: stats?.inFlight?.count ?? 0 })
  })

  // Migration jobs still waiting to run: the backlog plus, where the engine reports them
  // apart, jobs waiting for a retry.
  fastify.get('/progress', { schema: { tags: ['migration'] } }, async (_req, reply) => {
    if (!pgQueueEnable) {
      return reply.code(400).send({ message: 'Queue is not enabled' })
    }
    const stats = await migrationQueueStats()
    const remaining = (stats?.backlog?.count ?? 0) + (stats?.retryParked?.total.count ?? 0)
    return reply.send({ remaining })
  })

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
