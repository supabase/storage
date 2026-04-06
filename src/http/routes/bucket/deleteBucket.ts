import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { registerJsonParserAllowingEmptyBody } from '../../plugins/empty-json-body'
import { createDefaultSchema, createResponse } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const deleteBucketParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketId'],
} as const

const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string', examples: ['Successfully deleted'] },
  },
}
interface deleteBucketRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof deleteBucketParamsSchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Delete a bucket'
  const schema = createDefaultSchema(successResponseSchema, {
    params: deleteBucketParamsSchema,
    summary,
    tags: ['bucket'],
  })

  fastify.register(async (f) => {
    registerJsonParserAllowingEmptyBody(f)

    f.delete<deleteBucketRequestInterface>(
      '/:bucketId',
      {
        schema,
        config: {
          operation: { type: ROUTE_OPERATIONS.DELETE_BUCKET },
        },
      },
      async (request, response) => {
        const { bucketId } = request.params
        await request.storage.deleteBucket(bucketId)

        return response.status(200).send(createResponse('Successfully deleted'))
      }
    )
  })
}
