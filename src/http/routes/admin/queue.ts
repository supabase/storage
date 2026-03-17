import { MoveJobs, UpgradePgBossV10 } from '@storage/events'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import apiKey from '../../plugins/apikey'

const { pgQueueEnable } = getConfig()
// Empty ref/host mark system-scoped events and intentionally skip per-tenant shouldSend lookups.
const systemTenant = {
  ref: '',
  host: '',
} as const

const moveJobsSchema = {
  body: {
    type: 'object',
    properties: {
      fromQueue: {
        type: 'string',
      },
      toQueue: {
        type: 'string',
      },
      deleteJobsFromOriginalQueue: {
        type: 'boolean',
        default: false,
      },
    },
    required: ['fromQueue', 'toQueue'],
  },
} as const

interface MoveJobsRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof moveJobsSchema.body>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.post('/migrate/pgboss-v10', { schema: { tags: ['queue'] } }, async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.status(400).send({ message: 'Queue is not enabled' })
    }

    await UpgradePgBossV10.send({
      tenant: systemTenant,
    })

    return reply.send({ message: 'Migration scheduled' })
  })

  fastify.post<MoveJobsRequestInterface>(
    '/move',
    { schema: { tags: ['queue'] } },
    async (req, reply) => {
      if (!pgQueueEnable) {
        return reply.status(400).send({ message: 'Queue is not enabled' })
      }

      const fromQueue = req.body.fromQueue
      const toQueue = req.body.toQueue
      const deleteJobsFromOriginalQueue = req.body.deleteJobsFromOriginalQueue || false

      await MoveJobs.send({
        fromQueue,
        toQueue,
        deleteJobsFromOriginalQueue,
        tenant: systemTenant,
      })

      return reply.send({ message: 'Move jobs scheduled' })
    }
  )
}
