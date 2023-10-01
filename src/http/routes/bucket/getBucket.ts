import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../generic-routes'
import { bucketSchema } from '../../../storage/schemas'
import { AuthenticatedRequest } from '../../request'

const getBucketParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketId'],
} as const

const successResponseSchema = bucketSchema
interface getBucketRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getBucketParamsSchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Get details of a bucket'
  const schema = createDefaultSchema(successResponseSchema, {
    params: getBucketParamsSchema,
    summary,
    tags: ['bucket'],
  })
  fastify.get<getBucketRequestInterface>(
    '/:bucketId',
    {
      schema,
    },
    async (request, response) => {
      const { bucketId } = request.params

      const results = await request.storage.findBucket(
        bucketId,
        'id, name, owner, public, created_at, updated_at, file_size_limit, allowed_mime_types'
      )

      response.send(results)
    }
  )
}
