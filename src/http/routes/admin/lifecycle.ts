import { LifecycleTenantStorePg, multitenantPgExecutor } from '@internal/database'
import { logSchema } from '@internal/monitoring'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { registerApiKeyAuth } from '../../plugins/apikey'
import { registerJsonParserAllowingEmptyBody } from '../../plugins/empty-json-body'

export const LIFECYCLE_TENANT_RUN_MAX_BATCH_SIZE = 1000
const TENANT_ID_PATTERN = '^[^\\u0000]+$'

const errorResponseSchema = {
  type: 'object',
  additionalProperties: true,
  properties: {
    error: { type: 'string' },
    message: { type: 'string' },
  },
} as const

const scheduleTenantRunSchema = {
  description:
    'Wake the lifecycle tenant dispatcher for a tenant so the scheduler dispatches it within the fleet lease limit. Only eligible or previously enrolled tenants are woken, and only due buckets are executed.',
  params: {
    type: 'object',
    properties: {
      tenantId: { type: 'string', minLength: 1, pattern: TENANT_ID_PATTERN },
    },
    required: ['tenantId'],
  },
  response: {
    202: {
      type: 'object',
      properties: {
        message: { type: 'string' },
        tenantId: { type: 'string' },
      },
      required: ['message', 'tenantId'],
    },
    400: errorResponseSchema,
    404: {
      type: 'object',
      properties: {
        error: { type: 'string' },
      },
      required: ['error'],
    },
  },
} as const

const scheduleTenantRunBatchSchema = {
  description:
    'Wake the lifecycle tenant dispatcher for up to 1000 tenants so the scheduler dispatches them within the fleet lease limit. Only eligible or previously enrolled tenants are woken, and only due buckets are executed.',
  body: {
    type: 'object',
    additionalProperties: false,
    properties: {
      tenantIds: {
        type: 'array',
        minItems: 1,
        maxItems: LIFECYCLE_TENANT_RUN_MAX_BATCH_SIZE,
        items: { type: 'string', minLength: 1, pattern: TENANT_ID_PATTERN },
      },
    },
    required: ['tenantIds'],
  },
  response: {
    202: {
      type: 'object',
      properties: {
        message: { type: 'string' },
        scheduledTenantIds: { type: 'array', items: { type: 'string' } },
        disabledTenantIds: { type: 'array', items: { type: 'string' } },
        missingTenantIds: { type: 'array', items: { type: 'string' } },
      },
      required: ['message', 'scheduledTenantIds', 'disabledTenantIds', 'missingTenantIds'],
    },
    400: {
      ...errorResponseSchema,
      properties: {
        ...errorResponseSchema.properties,
        disabledTenantIds: { type: 'array', items: { type: 'string' } },
        missingTenantIds: { type: 'array', items: { type: 'string' } },
      },
    },
    404: {
      type: 'object',
      properties: {
        error: { type: 'string' },
        disabledTenantIds: { type: 'array', items: { type: 'string' } },
        missingTenantIds: { type: 'array', items: { type: 'string' } },
      },
      required: ['error', 'disabledTenantIds', 'missingTenantIds'],
    },
  },
} as const

interface ScheduleTenantRunRequest extends RequestGenericInterface {
  Params: FromSchema<typeof scheduleTenantRunSchema.params>
}

interface ScheduleTenantRunBatchRequest extends RequestGenericInterface {
  Body: FromSchema<typeof scheduleTenantRunBatchSchema.body>
}

const lifecycleTenantStorePg = new LifecycleTenantStorePg(multitenantPgExecutor)

function getLifecycleSchedulingError(config: {
  isMultitenant: boolean
  pgQueueEnable: boolean
  storageLifecyclePaused: boolean
}): string | undefined {
  if (!config.isMultitenant) {
    return 'This endpoint only supports multitenant mode'
  }

  if (!config.pgQueueEnable) {
    return 'Queue is not enabled'
  }

  if (config.storageLifecyclePaused) {
    return 'Lifecycle execution is paused'
  }

  return undefined
}

function buildWakeSignal(requestSignal?: AbortSignal, timeoutMs?: number): AbortSignal | undefined {
  const timeoutSignal = timeoutMs && timeoutMs > 0 ? AbortSignal.timeout(timeoutMs) : undefined
  if (requestSignal && timeoutSignal) {
    return AbortSignal.any([requestSignal, timeoutSignal])
  }
  return requestSignal ?? timeoutSignal
}

export default async function routes(fastify: FastifyInstance) {
  registerApiKeyAuth(fastify)
  registerJsonParserAllowingEmptyBody(fastify)

  fastify.post<ScheduleTenantRunBatchRequest>(
    '/lifecycle/run',
    {
      schema: { ...scheduleTenantRunBatchSchema, tags: ['lifecycle'] },
    },
    async (request, reply) => {
      const config = getConfig()
      const schedulingError = getLifecycleSchedulingError(config)
      if (schedulingError) {
        return reply.status(400).send({ error: schedulingError })
      }

      const uniqueTenantIds = [...new Set(request.body.tenantIds)]
      const wakeSignal = buildWakeSignal(
        request.signals?.disconnect.signal,
        config.multitenantDatabaseQueryTimeout
      )
      const { wokenTenantIds, disabledTenantIds: disabledIds } =
        await lifecycleTenantStorePg.wakeTenants({
          tenantIds: uniqueTenantIds,
          storageVersioningEnabled: config.storageVersioningEnabled,
          signal: wakeSignal,
        })

      const wokenSet = new Set(wokenTenantIds)
      const disabledSet = new Set(disabledIds)
      const scheduledTenantIds: string[] = []
      const disabledTenantIds: string[] = []
      const missingTenantIds: string[] = []
      for (const tenantId of uniqueTenantIds) {
        if (wokenSet.has(tenantId)) {
          scheduledTenantIds.push(tenantId)
        } else if (disabledSet.has(tenantId)) {
          disabledTenantIds.push(tenantId)
        } else {
          missingTenantIds.push(tenantId)
        }
      }

      if (scheduledTenantIds.length === 0) {
        if (disabledTenantIds.length > 0) {
          return reply.status(400).send({
            error: 'Lifecycle is not enabled for any matching tenants',
            disabledTenantIds,
            missingTenantIds,
          })
        }
        return reply.status(404).send({
          error: 'No matching tenants found',
          disabledTenantIds,
          missingTenantIds,
        })
      }

      logSchema.info(request.log, '[AdminLifecycle] Woke lifecycle tenants', {
        type: 'lifecycle-rule',
        reqId: request.id,
        sbReqId: request.sbReqId,
        metadata: JSON.stringify({
          requestedCount: uniqueTenantIds.length,
          scheduledCount: scheduledTenantIds.length,
          disabledCount: disabledTenantIds.length,
          missingCount: missingTenantIds.length,
        }),
      })

      return reply.status(202).send({
        message: 'Lifecycle tenant runs scheduled',
        scheduledTenantIds,
        disabledTenantIds,
        missingTenantIds,
      })
    }
  )

  fastify.post<ScheduleTenantRunRequest>(
    '/:tenantId/lifecycle/run',
    {
      schema: { ...scheduleTenantRunSchema, tags: ['lifecycle'] },
    },
    async (request, reply) => {
      const config = getConfig()
      const schedulingError = getLifecycleSchedulingError(config)
      if (schedulingError) {
        return reply.status(400).send({ error: schedulingError })
      }

      const { tenantId } = request.params
      const wakeSignal = buildWakeSignal(
        request.signals?.disconnect.signal,
        config.multitenantDatabaseQueryTimeout
      )
      const { wokenTenantIds, disabledTenantIds } = await lifecycleTenantStorePg.wakeTenants({
        tenantIds: [tenantId],
        storageVersioningEnabled: config.storageVersioningEnabled,
        signal: wakeSignal,
      })
      if (wokenTenantIds.length === 0) {
        if (disabledTenantIds.length > 0) {
          return reply.status(400).send({ error: 'Lifecycle is not enabled for tenant' })
        }
        return reply.status(404).send({ error: 'Tenant not found' })
      }

      logSchema.info(request.log, '[AdminLifecycle] Woke lifecycle tenant', {
        type: 'lifecycle-rule',
        tenantId,
        project: tenantId,
        reqId: request.id,
        sbReqId: request.sbReqId,
      })

      return reply.status(202).send({
        message: 'Lifecycle tenant run scheduled',
        tenantId,
      })
    }
  )
}
