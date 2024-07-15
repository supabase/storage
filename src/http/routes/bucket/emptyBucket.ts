import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const emptyBucketParamsSchema = {
  type: 'object',
  properties: {
    Bucket: { type: 'string', examples: ['avatars'] },
  },
  required: ['Bucket'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string', examples: ['Successfully emptied'] },
  },
}
interface emptyBucketRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof emptyBucketParamsSchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Empty a bucket'
  const schema = createDefaultSchema(successResponseSchema, {
    params: emptyBucketParamsSchema,
    summary,
    tags: ['bucket'],
  })
  fastify.post<emptyBucketRequestInterface>(
    '/:Bucket/empty',
    {
      schema,
      config: {
        operation: { type: ROUTE_OPERATIONS.EMPTY_BUCKET },
      },
    },
    async (request, response) => {
      const { Bucket } = request.params

      await request.storage.emptyBucket(Bucket)

      return response.status(200).send(createResponse('Successfully emptied'))
    }
  )
}
