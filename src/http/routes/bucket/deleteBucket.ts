import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'

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
  fastify.delete<deleteBucketRequestInterface>(
    '/:bucketId',
    {
      schema,
    },
    async (request, response) => {
      const { bucketId } = request.params

      const bucket = await request.storage.findBucket(bucketId)
      request.log.info({ results: bucket }, 'results')

      const deletedBucket = await request.storage.deleteBucket(bucketId)

      request.log.info({ results: deletedBucket }, 'results')
      return response.status(200).send(createResponse('Successfully deleted'))
    }
  )
}
