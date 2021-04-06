import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { bucketSchema } from '../../schemas/bucket'
import { AuthenticatedRequest, Bucket } from '../../types/types'
import { getPostgrestClient, transformPostgrestError } from '../../utils'
import { createDefaultSchema } from '../../utils/generic-routes'

const getBucketParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', example: 'avatars' },
  },
  required: ['bucketId'],
} as const

const successResponseSchema = bucketSchema
interface getBucketRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getBucketParamsSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
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
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)
      const { bucketId } = request.params
      const postgrest = getPostgrestClient(jwt)
      const { data: results, error, status } = await postgrest
        .from<Bucket>('buckets')
        .select('id, name, owner, created_at, updated_at')
        .eq('id', bucketId)
        .single()

      if (error) {
        request.log.error({ error }, 'error bucket')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      request.log.info({ results }, 'results')

      response.send(results)
    }
  )
}
