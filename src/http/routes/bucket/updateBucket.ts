import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const updateBucketBodySchema = {
  type: 'object',
  properties: {
    public: { type: 'boolean', examples: [false] },
    file_size_limit: {
      anyOf: [
        { type: 'integer', examples: [1000], nullable: true, minimum: 0 },
        { type: 'string', examples: ['100MB'], nullable: true },
      ],
    },
    allowed_mime_types: {
      type: 'array',
      nullable: true,
      items: { type: 'string', examples: [['image/png', 'image/jpg']] },
    },
  },
} as const
const updateBucketParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketId'],
} as const

const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string', examples: ['Successfully updated'] },
  },
  required: ['message'],
}
interface updateBucketRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof updateBucketBodySchema>
  Params: FromSchema<typeof updateBucketParamsSchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Update properties of a bucket'
  const schema = createDefaultSchema(successResponseSchema, {
    body: updateBucketBodySchema,
    summary,
    tags: ['bucket'],
  })
  fastify.put<updateBucketRequestInterface>(
    '/:bucketId',
    {
      schema,
      config: {
        operation: { type: ROUTE_OPERATIONS.UPDATE_BUCKET },
      },
    },
    async (request, response) => {
      const { bucketId } = request.params

      const { public: isPublic, file_size_limit, allowed_mime_types } = request.body

      await request.storage.updateBucket(bucketId, {
        public: isPublic,
        fileSizeLimit: file_size_limit,
        allowedMimeTypes: allowed_mime_types
          ? allowed_mime_types?.filter((mime) => mime)
          : allowed_mime_types,
      })

      return response.status(200).send(createResponse('Successfully updated'))
    }
  )
}
