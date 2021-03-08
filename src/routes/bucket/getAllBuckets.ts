import { FastifyInstance } from 'fastify'
import { getPostgrestClient } from '../../utils'
import { AuthenticatedRequest, Bucket } from '../../types/types'
// @todo change later
const successResponseSchema = {
  type: 'array',
  items: {
    type: 'object',
    properties: {
      id: { type: 'string' },
      name: { type: 'string' },
      owner: { type: 'string' },
      createdAt: { type: 'string' },
      updatedAt: { type: 'string' },
    },
    required: ['id', 'name'],
  },
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Gets all buckets'
  // @todo I have enabled RLS only for objects table
  // makes writing the access policies a bit easier
  // tradeoff is that everyone can see what buckets there are
  // ahh looks like we do need RLS since users would be able to delete and empty buckets then
  // probably a RLS policy with just read permissions?
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
      if (!authHeader) {
        return response.status(403).send('Go away')
      }
      const jwt = authHeader.substring('Bearer '.length)

      const postgrest = getPostgrestClient(jwt)
      const { data: results, error, status } = await postgrest.from<Bucket>('buckets').select('*')
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
