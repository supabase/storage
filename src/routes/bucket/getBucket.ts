import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { bucketSchema } from '../../schemas/bucket'
import { AuthenticatedRequest, Bucket } from '../../types/types'
import { transformPostgrestError } from '../../utils'
import { createDefaultSchema } from '../../utils/generic-routes'

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
      const { bucketId } = request.params
      const {
        data: results,
        error,
        status,
      } = await request.postgrest
        .from<Bucket>('buckets')
        .select('id, name, owner, public, created_at, updated_at')
        .eq('id', bucketId)
        .single()

      if (error) {
        request.log.error({ error, bucketId }, 'failed to retrieve bucket info')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      request.log.info({ results }, 'results')

      response.send(results)
    }
  )
}
