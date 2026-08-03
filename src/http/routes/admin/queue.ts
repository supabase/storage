import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { registerApiKeyAuth } from '../../plugins/apikey'

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
  registerApiKeyAuth(fastify)

  // MoveJobs was a v1 (pgboss_v10) maintenance tool for renaming queues / changing policies
  // in place. The current queue provisions its own queues declaratively; a v12-aware move
  // tool can be reintroduced when a real migration needs it.
  fastify.post<MoveJobsRequestInterface>(
    '/move',
    { schema: { ...moveJobsSchema, tags: ['queue'] } },
    async (_req, reply) => {
      return reply
        .status(501)
        .send({ message: 'MoveJobs is not available (v1-only maintenance tool)' })
    }
  )
}
