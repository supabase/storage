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
// @todo change later
const successResponseSchema = {
  type: 'object',
  properties: {
    id: { type: 'string' },
    name: { type: 'string' },
    owner: { type: 'string' },
    createdAt: { type: 'string' },
    updatedAt: { type: 'string' },
  },
  required: ['id', 'name'],
}
interface getBucketRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getBucketParamsSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Get details of a bucket'
  fastify.get<getBucketRequestInterface>(
    '/:bucketId',
    {
      schema: {
        params: getBucketParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { 200: successResponseSchema, '4xx': { $ref: 'errorSchema#' } },
      },
    },
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
        return response
          .status(status)
          .send({ statusCode: error.code, error: error.details, message: error.message })
      }

      response.send(results)
    }
  )
}
