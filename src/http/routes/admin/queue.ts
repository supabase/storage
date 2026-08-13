import { SYSTEM_TENANT } from '@internal/queue'
import {
  getStorageQueue,
  MoveJobsToPgboss,
  MoveJobsToPgque,
  MoveJobsV10ToV12,
  MoveJobsV12ToV10,
} from '@storage/events'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { registerApiKeyAuth } from '../../plugins/apikey'

const { pgQueueEnable, pgQueueAdapter } = getConfig()

const moveJobsSchema = {
  body: {
    type: 'object',
    properties: {
      // Where to move jobs INTO:
      // - 'pgque' / 'pgboss': engine cutover — drain the OTHER engine into the running one,
      //   so the destination must be the active adapter.
      // - 'pgboss-v12' / 'pgboss-v10': schema-generation moves between the v1 fleet's
      //   pgboss_v10 and v2's pgboss_v12 — direct SQL on both sides, valid under either
      //   adapter (moving off a v10 deploy onto a pgque fleet is 'pgboss-v12' then 'pgque').
      to: {
        type: 'string',
        enum: ['pgque', 'pgboss', 'pgboss-v12', 'pgboss-v10'],
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

  // v2 reshape of the v1 MoveJobs maintenance tool: instead of copying rows between pg-boss
  // queues, it schedules a backlog move between queue backends (see
  // storage/events/upgrades/move-jobs.ts).
  fastify.post<MoveJobsRequestInterface>(
    '/move',
    { schema: { ...moveJobsSchema, tags: ['queue'] } },
    async (req, reply) => {
      if (!pgQueueEnable) {
        return reply.status(400).send({ message: 'Queue is not enabled' })
      }

      const base = { sbReqId: req.sbReqId, tenant: SYSTEM_TENANT }
      let event: MoveJobsToPgque | MoveJobsToPgboss | MoveJobsV10ToV12 | MoveJobsV12ToV10

      switch (req.body.to) {
        case 'pgque':
        case 'pgboss': {
          if (req.body.to !== pgQueueAdapter) {
            return reply.status(400).send({
              message: `Cannot move jobs to '${req.body.to}': the running queue adapter is '${pgQueueAdapter}'. Jobs can only be moved into the active engine.`,
            })
          }
          event = req.body.to === 'pgque' ? new MoveJobsToPgque(base) : new MoveJobsToPgboss(base)
          break
        }
        case 'pgboss-v12':
          event = new MoveJobsV10ToV12(base)
          break
        case 'pgboss-v10':
          event = new MoveJobsV12ToV10(base)
          break
      }

      await getStorageQueue().produce(event)

      return reply.send({ message: `Move jobs to ${req.body.to} scheduled` })
    }
  )
}
