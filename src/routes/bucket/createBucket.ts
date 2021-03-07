import { FastifyInstance } from 'fastify'
import { getPostgrestClient, getOwner } from '../../utils'
import { AuthenticatedRequest, Bucket } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

const createBucketBodySchema = {
  type: 'object',
  properties: {
    name: { type: 'string' },
  },
  required: ['bucketName'],
} as const
interface createBucketRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof createBucketBodySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  fastify.post<createBucketRequestInterface>(
    '/',
    { schema: { body: createBucketBodySchema, headers: { $ref: 'authSchema#' } } },
    async (request, response) => {
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)
      const postgrest = getPostgrestClient(jwt)
      const owner = await getOwner(jwt)

      const { name: bucketName } = request.body

      const { data: results, error } = await postgrest.from<Bucket>('buckets').insert([
        {
          name: bucketName,
          owner,
        },
      ])
      console.log(results, error)
      return response.status(200).send(results)
    }
  )
}
