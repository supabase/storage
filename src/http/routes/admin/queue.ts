import { SYSTEM_TENANT } from '@internal/queue/constants'
import { MoveJobs, UpgradePgBossV10 } from '@storage/events'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import {
  backupQueueOverflow,
  JOB_OVERFLOW_LIST_LIMIT_DEFAULT,
  JOB_OVERFLOW_RESTORE_LIMIT_DEFAULT,
  listQueueOverflow,
  parseQueueOverflowCsv,
  restoreQueueOverflow,
} from '../../../internal/queue/overflow'
import apiKey from '../../plugins/apikey'

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

const listQueueOverflowSchema = {
  description: 'List created pgBoss jobs from the live queue table or overflow backup table.',
  querystring: {
    type: 'object',
    properties: {
      source: {
        type: 'string',
        enum: ['job', 'backup'],
        default: 'job',
      },
      groupBy: {
        type: 'string',
        enum: ['summary', 'tenant'],
        default: 'summary',
      },
      name: {
        type: 'string',
        minLength: 1,
      },
      eventTypes: {
        type: 'string',
        minLength: 1,
        description: 'Comma-separated event types to filter on.',
      },
      tenantRefs: {
        type: 'string',
        minLength: 1,
        description: 'Comma-separated tenant refs to filter on.',
      },
      limit: {
        type: 'integer',
        minimum: 1,
        default: JOB_OVERFLOW_LIST_LIMIT_DEFAULT,
      },
    },
    additionalProperties: false,
  },
} as const

const backupQueueOverflowSchema = {
  description: 'Move created pgBoss jobs into the overflow backup table.',
  body: {
    type: 'object',
    properties: {
      name: {
        type: 'string',
        minLength: 1,
      },
      eventTypes: {
        type: 'array',
        minItems: 1,
        items: {
          type: 'string',
          minLength: 1,
        },
      },
      tenantRefs: {
        type: 'array',
        minItems: 1,
        items: {
          type: 'string',
          minLength: 1,
        },
      },
      limit: {
        type: 'integer',
        minimum: 1,
      },
    },
    additionalProperties: false,
  },
} as const

const restoreQueueOverflowSchema = {
  description: 'Restore created pgBoss jobs from the overflow backup table in batches.',
  body: {
    type: 'object',
    properties: {
      name: {
        type: 'string',
        minLength: 1,
      },
      eventTypes: {
        type: 'array',
        minItems: 1,
        items: {
          type: 'string',
          minLength: 1,
        },
      },
      tenantRefs: {
        type: 'array',
        minItems: 1,
        items: {
          type: 'string',
          minLength: 1,
        },
      },
      limit: {
        type: 'integer',
        minimum: 1,
        default: JOB_OVERFLOW_RESTORE_LIMIT_DEFAULT,
      },
    },
    additionalProperties: false,
  },
} as const

interface MoveJobsRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof moveJobsSchema.body>
}

interface ListQueueOverflowRequestInterface extends RequestGenericInterface {
  Querystring: FromSchema<typeof listQueueOverflowSchema.querystring>
}

interface BackupQueueOverflowRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof backupQueueOverflowSchema.body>
}

interface RestoreQueueOverflowRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof restoreQueueOverflowSchema.body>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.post('/migrate/pgboss-v10', { schema: { tags: ['queue'] } }, async (req, reply) => {
    if (!pgQueueEnable) {
      return reply.status(400).send({ message: 'Queue is not enabled' })
    }

    await UpgradePgBossV10.send({
      sbReqId: req.sbReqId,
      tenant: SYSTEM_TENANT,
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
        sbReqId: req.sbReqId,
        tenant: SYSTEM_TENANT,
      })

      return reply.send({ message: 'Move jobs scheduled' })
    }
  )

  fastify.get<ListQueueOverflowRequestInterface>(
    '/overflow',
    { schema: { ...listQueueOverflowSchema, tags: ['queue'] } },
    async (req, reply) => {
      const data = await listQueueOverflow({
        source: req.query.source,
        groupBy: req.query.groupBy,
        name: req.query.name,
        eventTypes: parseQueueOverflowCsv(req.query.eventTypes),
        tenantRefs: parseQueueOverflowCsv(req.query.tenantRefs),
        limit: req.query.limit,
      })

      return reply.send(data)
    }
  )

  fastify.post<BackupQueueOverflowRequestInterface>(
    '/overflow/backup',
    { schema: { ...backupQueueOverflowSchema, tags: ['queue'] } },
    async (req, reply) => {
      const data = await backupQueueOverflow(req.body)
      return reply.send(data)
    }
  )

  fastify.post<RestoreQueueOverflowRequestInterface>(
    '/overflow/restore',
    { schema: { ...restoreQueueOverflowSchema, tags: ['queue'] } },
    async (req, reply) => {
      const data = await restoreQueueOverflow(req.body)
      return reply.send(data)
    }
  )
}
