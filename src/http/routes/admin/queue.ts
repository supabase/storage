import { FastifyInstance, RequestGenericInterface } from 'fastify'
import apiKey from '../../plugins/apikey'
import { getConfig } from '../../../config'
import { MoveJobs, UpgradePgBossV10 } from '@storage/events'
import { FromSchema } from 'json-schema-to-ts'

const { pgQueueEnable } = getConfig()

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

  fastify.post('/migrate/pgboss-v10', async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.status(400).send({ message: 'Queue is not enabled' })
    }

    await UpgradePgBossV10.send({})

    return reply.send({ message: 'Migration scheduled' })
  })

  fastify.post<MoveJobsRequestInterface>('/move', async (req, reply) => {
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
    })

    return reply.send({ message: 'Move jobs scheduled' })
  })
}
