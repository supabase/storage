import { SYSTEM_TENANT } from '@internal/queue'
import {
  getStorageQueue,
  MOVE_TO_SQS_LIFETIME_MS,
  MOVE_TO_SQS_MAX_LANES,
  MoveJobsV10ToSqs,
  MoveJobsV10ToV12,
  MoveJobsV12ToV10,
} from '@storage/events'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { registerApiKeyAuth } from '../../plugins/apikey'

const { pgQueueEnable, queueAdapter } = getConfig()

const moveJobsSchema = {
  body: {
    type: 'object',
    properties: {
      // Where to move jobs INTO. `pgboss-v12` / `pgboss-v10` are schema-generation moves
      // between the v1 fleet's pgboss_v10 and v2's pgboss_v12, direct SQL on both sides.
      // `sqs` drains pgboss_v10 into SQS (see storage/events/upgrades/move-jobs-sqs.ts).
      to: {
        type: 'string',
        enum: ['pgboss-v12', 'pgboss-v10', 'sqs'],
      },
      // The rest applies to `sqs` only.
      // Lanes moving at once. Each is a chain of jobs, one round per job.
      parallelism: { type: 'integer', minimum: 1, maximum: MOVE_TO_SQS_MAX_LANES },
      // Jobs one round moves at most: also the most a crashed lane can send twice.
      batchSize: { type: 'integer', minimum: 10, maximum: 1000 },
      // The pause between two rounds of a lane.
      delaySeconds: { type: 'number', minimum: 0, maximum: 900 },
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
      const base = { sbReqId: req.sbReqId, tenant: SYSTEM_TENANT }

      if (req.body.to === 'sqs') {
        if (queueAdapter !== 'sqs') {
          return reply.status(400).send({
            message: `Cannot move jobs to SQS: the running queue adapter is '${queueAdapter}'.`,
          })
        }
        // 3 lanes of 500 jobs every 5s is 300 jobs a second: a 100k backlog in under 6 minutes.
        const { parallelism = 3, batchSize = 500, delaySeconds = 5 } = req.body
        const expiresAt = Date.now() + MOVE_TO_SQS_LIFETIME_MS
        const lanes = Array.from(
          { length: parallelism },
          (_, lane) =>
            new MoveJobsV10ToSqs({
              ...base,
              lane,
              batchSize,
              delayMs: Math.round(delaySeconds * 1000),
              // Lanes start on different queues, so the first round spreads over them.
              round: lane,
              expiresAt,
            })
        )
        await getStorageQueue().produce(lanes)

        return reply.send({
          message: `Move jobs to sqs scheduled on ${parallelism} lanes (${batchSize} jobs every ${delaySeconds}s each)`,
        })
      }

      if (queueAdapter !== 'pgboss') {
        return reply.status(400).send({
          message: `Cannot move jobs: the running queue adapter is '${queueAdapter}'. Moves are between pg-boss schemas.`,
        })
      }

      const event =
        req.body.to === 'pgboss-v12' ? new MoveJobsV10ToV12(base) : new MoveJobsV12ToV10(base)

      await getStorageQueue().produce(event)

      return reply.send({ message: `Move jobs to ${req.body.to} scheduled` })
    }
  )
}
