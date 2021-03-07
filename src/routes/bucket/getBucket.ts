import { FastifyInstance } from 'fastify'
import { getPostgrestClient } from '../../utils'
import { AuthenticatedRequest, Bucket } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

const getBucketParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string' },
    '*': { type: 'string' },
  },
  required: ['bucketId', '*'],
} as const
interface getBucketRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getBucketParamsSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  fastify.get<getBucketRequestInterface>(
    '/:bucketId',
    { schema: { params: getBucketParamsSchema, headers: { $ref: 'authSchema#' } } },
    async (request, response) => {
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)
      const { bucketId } = request.params
      const postgrest = getPostgrestClient(jwt)
      const { data: results, error, status } = await postgrest
        .from<Bucket>('buckets')
        .select('*')
        .eq('id', bucketId)
        .single()

      console.log(results, error)

      if (error) {
        return response.status(status).send(error.message)
      }

      response.send(results)
    }
  )
}
