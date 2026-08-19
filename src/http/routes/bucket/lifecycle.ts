import { ERRORS } from '@internal/errors'
import {
  assertLifecycleApiEnabled,
  assertLifecycleSchemaReady,
  LifecycleConfigurationValidationError,
  normalizeLifecycleConfiguration,
} from '@storage/lifecycle'
import { bucketLifecycleConfigurationSchema } from '@storage/schemas/lifecycle'
import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { registerJsonParserAllowingEmptyBody } from '../../plugins/empty-json-body'
import { createDefaultSchema, createResponse } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const lifecycleParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketId'],
} as const

interface LifecycleRequest extends AuthenticatedRequest {
  Params: FromSchema<typeof lifecycleParamsSchema>
}

interface PutLifecycleRequest extends LifecycleRequest {
  Body: FromSchema<typeof bucketLifecycleConfigurationSchema>
}

const deleteSuccessResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string', examples: ['Successfully deleted'] },
  },
  required: ['message'],
}

function normalizeRestLifecycleConfiguration(input: unknown) {
  try {
    return normalizeLifecycleConfiguration(input)
  } catch (error) {
    if (error instanceof LifecycleConfigurationValidationError) {
      // REST keeps its parameter-validation envelope. The S3 adapter maps the
      // same validator categories to the corresponding AWS error codes.
      throw ERRORS.InvalidParameter('lifecycle_configuration', {
        error,
        message: error.message,
      })
    }
    throw error
  }
}

export default async function routes(fastify: FastifyInstance) {
  const getSchema = createDefaultSchema(bucketLifecycleConfigurationSchema, {
    params: lifecycleParamsSchema,
    summary: 'Get a bucket lifecycle configuration',
    tags: ['bucket'],
  })
  const putSchema = createDefaultSchema(bucketLifecycleConfigurationSchema, {
    body: bucketLifecycleConfigurationSchema,
    description:
      'The full configuration replaces any existing policy. Semantic validation failures use the REST InvalidParameter error contract.',
    params: lifecycleParamsSchema,
    summary: 'Replace a bucket lifecycle configuration',
    tags: ['bucket'],
  })
  const deleteSchema = createDefaultSchema(deleteSuccessResponseSchema, {
    params: lifecycleParamsSchema,
    summary: 'Delete a bucket lifecycle configuration',
    tags: ['bucket'],
  })

  fastify.register(async (scopedFastify) => {
    registerJsonParserAllowingEmptyBody(scopedFastify)

    scopedFastify.get<LifecycleRequest>(
      '/:bucketId/lifecycle',
      {
        schema: getSchema,
        config: { operation: ROUTE_OPERATIONS.GET_BUCKET_LIFECYCLE },
      },
      async (request, response) => {
        const { bucketId } = request.params
        assertLifecycleApiEnabled(bucketId)

        const configuration = await request.storage.getBucketLifecycle(bucketId)
        if (!configuration) {
          throw ERRORS.NoSuchLifecycleConfiguration(bucketId)
        }

        return response.send(configuration)
      }
    )

    scopedFastify.put<PutLifecycleRequest>(
      '/:bucketId/lifecycle',
      {
        schema: putSchema,
        config: { operation: ROUTE_OPERATIONS.PUT_BUCKET_LIFECYCLE },
      },
      async (request, response) => {
        const { bucketId } = request.params
        assertLifecycleApiEnabled(bucketId)
        await assertLifecycleSchemaReady(request.storage, bucketId)
        const configuration = normalizeRestLifecycleConfiguration(request.body)
        const storedConfiguration = await request.storage.putBucketLifecycle(
          bucketId,
          configuration
        )

        return response.send(storedConfiguration)
      }
    )

    scopedFastify.delete<LifecycleRequest>(
      '/:bucketId/lifecycle',
      {
        schema: deleteSchema,
        config: { operation: ROUTE_OPERATIONS.DELETE_BUCKET_LIFECYCLE },
      },
      async (request, response) => {
        const { bucketId } = request.params
        assertLifecycleApiEnabled(bucketId)
        await request.storage.deleteBucketLifecycle(bucketId)

        return response.status(200).send(createResponse('Successfully deleted'))
      }
    )
  })
}
