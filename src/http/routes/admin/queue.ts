import { SYSTEM_TENANT } from '@internal/queue'
import { getStorageQueue, MoveJobsV10ToV12, MoveJobsV12ToV10 } from '@storage/events'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { registerApiKeyAuth } from '../../plugins/apikey'

const { pgQueueEnable, queueAdapter } = getConfig()

const moveJobsSchema = {
  body: {
    type: 'object',
    properties: {
      // Where to move jobs INTO: schema-generation moves between the v1 fleet's pgboss_v10
      // and v2's pgboss_v12, direct SQL on both sides.
      to: {
        type: 'string',
        enum: ['pgboss-v12', 'pgboss-v10'],
      },
    },
    required: ['to'],
  },
} as const

interface MoveJobsRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof moveJobsSchema.body>
}

export default async function routes(fastify: FastifyInstance) {
  registerApiKeyAuth(fastify)

  // v2 reshape of the v1 MoveJobs maintenance tool: schedules a backlog move between the
  // pg-boss schema generations (see storage/events/upgrades/move-jobs.ts).
  fastify.post<MoveJobsRequestInterface>(
    '/move',
    { schema: { ...moveJobsSchema, tags: ['queue'] } },
    async (req, reply) => {
      if (!pgQueueEnable) {
        return reply.status(400).send({ message: 'Queue is not enabled' })
      }
      if (queueAdapter !== 'pgboss') {
        return reply.status(400).send({
          message: `Cannot move jobs: the running queue adapter is '${queueAdapter}'. Moves are between pg-boss schemas.`,
        })
      }

      const base = { sbReqId: req.sbReqId, tenant: SYSTEM_TENANT }
      const event =
        req.body.to === 'pgboss-v12' ? new MoveJobsV10ToV12(base) : new MoveJobsV12ToV10(base)

      await getStorageQueue().produce(event)

      return reply.send({ message: `Move jobs to ${req.body.to} scheduled` })
    }
  )
}
