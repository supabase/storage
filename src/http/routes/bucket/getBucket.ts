import { bucketSchema } from '@storage/schemas'
import { nullableBucketLifecycleConfigurationSchema } from '@storage/schemas/lifecycle'
import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { createDefaultSchema } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const { storageLifecycleEnabled } = getConfig()

const getBucketParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketId'],
} as const

const getBucketQuerySchema = {
  type: 'object',
  properties: {
    include: {
      type: 'string',
      enum: ['lifecycle'],
      description:
        'Include lifecycle_configuration when lifecycle support is available. The field is omitted when the feature is disabled.',
    },
  },
} as const

const successResponseSchema = {
  ...bucketSchema,
  $id: 'bucketWithLifecycleSchema',
  properties: {
    ...bucketSchema.properties,
    lifecycle_configuration: {
      ...nullableBucketLifecycleConfigurationSchema,
      description:
        'Returned only for include=lifecycle when lifecycle support is enabled. Null means no policy is stored or the tenant schema is not yet available.',
    },
  },
} as const
interface getBucketRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getBucketParamsSchema>
  Querystring: FromSchema<typeof getBucketQuerySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Get details of a bucket'
  const schema = createDefaultSchema(successResponseSchema, {
    params: getBucketParamsSchema,
    querystring: getBucketQuerySchema,
    summary,
    tags: ['bucket'],
  })
  fastify.get<getBucketRequestInterface>(
    '/:bucketId',
    {
      schema,
      config: {
        operation: ROUTE_OPERATIONS.GET_BUCKET,
      },
    },
    async (request, response) => {
      const { bucketId } = request.params
      const includeLifecycle = request.query.include === 'lifecycle'
      const supportsLifecycleConfiguration =
        includeLifecycle && storageLifecycleEnabled
          ? await request.storage.db.hasMigration('bucket-lifecycle-configuration')
          : false

      const results = await request.storage.findBucket(
        bucketId,
        'id, name, owner, public, created_at, updated_at, file_size_limit, allowed_mime_types' +
          (supportsLifecycleConfiguration ? ', lifecycle_configuration' : '')
      )

      if (!includeLifecycle || !storageLifecycleEnabled) return response.send(results)

      const storedLifecycleConfiguration = (
        results as typeof results & { lifecycle_configuration?: unknown }
      ).lifecycle_configuration

      return response.send({
        ...results,
        lifecycle_configuration: supportsLifecycleConfiguration
          ? (storedLifecycleConfiguration ?? null)
          : null,
      })
    }
  )
}
