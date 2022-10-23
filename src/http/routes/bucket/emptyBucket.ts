import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'

const emptyBucketParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketId'],
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
    '/:bucketId/empty',
    {
      schema,
    },
    async (request, response) => {
      const { bucketId } = request.params

      await request.storage.emptyBucket(bucketId)

      return response.status(200).send(createResponse('Successfully emptied'))
    }
  )
}
