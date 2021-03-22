import { FastifyInstance } from 'fastify'
import { getPostgrestClient, transformPostgrestError } from '../../utils'
import { AuthenticatedRequest, Bucket } from '../../types/types'
import { bucketSchema } from '../../schemas/bucket'

const successResponseSchema = {
  type: 'array',
  items: bucketSchema,
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Gets all buckets'
  fastify.get<AuthenticatedRequest>(
    '/',
    {
      schema: {
        headers: { $ref: 'authSchema#' },
        summary,
        response: { 200: successResponseSchema, '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      // get list of all buckets
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const postgrest = getPostgrestClient(jwt)
      const { data: results, error, status } = await postgrest
        .from<Bucket>('buckets')
        .select('id, name, owner, created_at, updated_at')
      console.log(results, error)

      if (error) {
        return response.status(400).send(transformPostgrestError(error, status))
      }

      response.send(results)
    }
  )
}
