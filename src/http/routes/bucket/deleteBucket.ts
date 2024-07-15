import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const deleteBucketParamsSchema = {
  type: 'object',
  properties: {
    Bucket: { type: 'string', examples: ['avatars'] },
  },
  required: ['Bucket'],
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
  fastify.delete<deleteBucketRequestInterface>(
    '/:Bucket',
    {
      schema,
      config: {
        operation: { type: ROUTE_OPERATIONS.DELETE_BUCKET },
      },
    },
    async (request, response) => {
      const { Bucket } = request.params
      await request.storage.deleteBucket(Bucket)

      return response.status(200).send(createResponse('Successfully deleted'))
    }
  )
}
