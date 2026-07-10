import { SYSTEM_TENANT } from '@internal/queue/constants'
import {
  JOB_OVERFLOW_LIST_LIMIT_DEFAULT,
  JOB_OVERFLOW_RESTORE_LIMIT_DEFAULT,
  parseCommaSeparatedList,
  QueueOverflowStorePg,
} from '@internal/queue/overflow'
import { Queue } from '@internal/queue/queue'
import { MoveJobs } from '@storage/events'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { registerApiKeyAuth } from '../../plugins/apikey'

const { pgQueueEnable } = getConfig()

function getQueueOverflowStore() {
  return new QueueOverflowStorePg(Queue.getDb())
}

const nonBlankStringSchema = {
  type: 'string',
  minLength: 1,
  pattern: '\\S',
} as const

const stringListSchema = {
  type: 'array',
  minItems: 1,
  maxItems: 1000,
  items: nonBlankStringSchema,
} as const

const positiveSafeIntegerSchema = {
  type: 'integer',
  finite: true,
  minimum: 1,
  maximum: Number.MAX_SAFE_INTEGER,
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
      name: nonBlankStringSchema,
      eventTypes: {
        ...nonBlankStringSchema,
        description: 'Comma-separated event types to filter on.',
      },
      tenantRefs: {
        ...nonBlankStringSchema,
        description: 'Comma-separated tenant refs to filter on.',
      },
      limit: {
        ...positiveSafeIntegerSchema,
        default: JOB_OVERFLOW_LIST_LIMIT_DEFAULT,
      },
    },
    additionalProperties: false,
  },
} as const

const countQueueOverflowSchema = {
  description: 'Count created pgBoss jobs in the live queue table.',
} as const

const backupQueueOverflowSchema = {
  description: 'Move created pgBoss jobs into the overflow backup table.',
  body: {
    type: 'object',
    properties: {
      name: nonBlankStringSchema,
      eventTypes: stringListSchema,
      tenantRefs: stringListSchema,
      limit: positiveSafeIntegerSchema,
      confirmAll: {
        type: 'boolean',
        description: 'Required when no queue, event type, or tenant filter is supplied.',
      },
    },
    anyOf: [
      { required: ['name'] },
      { required: ['eventTypes'] },
      { required: ['tenantRefs'] },
      {
        properties: {
          confirmAll: { type: 'boolean', enum: [true] },
        },
        required: ['confirmAll'],
      },
    ],
    additionalProperties: false,
  },
} as const

const restoreQueueOverflowSchema = {
  description:
    'Restore created pgBoss jobs from the overflow backup table in batches. Conflicting rows are dropped from the backup table because the live job table wins.',
  body: {
    type: 'object',
    properties: {
      name: nonBlankStringSchema,
      eventTypes: stringListSchema,
      tenantRefs: stringListSchema,
      limit: {
        ...positiveSafeIntegerSchema,
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
  registerApiKeyAuth(fastify)
  fastify.addHook('preHandler', async (_req, reply) => {
    if (!pgQueueEnable) {
      return reply.status(400).send({ message: 'Queue is not enabled' })
    }
  })

  fastify.post<MoveJobsRequestInterface>(
    '/move',
    { schema: { ...moveJobsSchema, tags: ['queue'] } },
    async (req, reply) => {
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
      const store = getQueueOverflowStore()
      const data = await store.list({
        source: req.query.source,
        groupBy: req.query.groupBy,
        name: req.query.name,
        eventTypes: parseCommaSeparatedList(req.query.eventTypes),
        tenantRefs: parseCommaSeparatedList(req.query.tenantRefs),
        limit: req.query.limit,
        signal: req.signals.disconnect.signal,
      })

      return reply.send(data)
    }
  )

  fastify.get(
    '/overflow/count',
    { schema: { ...countQueueOverflowSchema, tags: ['queue'] } },
    async (req, reply) => {
      const store = getQueueOverflowStore()
      const data = await store.countCreated({ signal: req.signals.disconnect.signal })
      return reply.send(data)
    }
  )

  fastify.post<BackupQueueOverflowRequestInterface>(
    '/overflow/backup',
    { schema: { ...backupQueueOverflowSchema, tags: ['queue'] } },
    async (req, reply) => {
      const store = getQueueOverflowStore()
      const data = await store.backup({
        ...req.body,
        signal: req.signals.disconnect.signal,
      })
      return reply.send(data)
    }
  )

  fastify.post<RestoreQueueOverflowRequestInterface>(
    '/overflow/restore',
    { schema: { ...restoreQueueOverflowSchema, tags: ['queue'] } },
    async (req, reply) => {
      const store = getQueueOverflowStore()
      const data = await store.restore({
        ...req.body,
        signal: req.signals.disconnect.signal,
      })
      return reply.send(data)
    }
  )
}
