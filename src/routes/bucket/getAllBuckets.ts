import { FastifyInstance } from 'fastify'
import { bucketSchema } from '../../schemas/bucket'
import { AuthenticatedRequest, Bucket } from '../../types/types'
import { getPostgrestClient, transformPostgrestError } from '../../utils'
import { createDefaultSchema } from '../../utils/generic-routes'

const successResponseSchema = {
  type: 'array',
  items: bucketSchema,
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Gets all buckets'
  const schema = createDefaultSchema(successResponseSchema, {
    summary,
  })

  fastify.get<AuthenticatedRequest>(
    '/',
    {
      schema,
    },
    async (request, response) => {
      // get list of all buckets
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const postgrest = getPostgrestClient(jwt)
      const { data: results, error, status } = await postgrest
        .from<Bucket>('buckets')
        .select('id, name, owner, created_at, updated_at')

      if (error) {
        request.log.error({ error }, 'error bucket')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      request.log.info({ results }, 'results')

      response.send(results)
    }
  )
}
