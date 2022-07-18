import { FastifyInstance } from 'fastify'
import { bucketSchema } from '../../schemas/bucket'
import { AuthenticatedRequest, Bucket } from '../../types/types'
import { transformPostgrestError } from '../../utils'
import { createDefaultSchema } from '../../utils/generic-routes'

const successResponseSchema = {
  type: 'array',
  items: bucketSchema,
  examples: [
    [
      {
        id: 'bucket2',
        name: 'bucket2',
        owner: '4d56e902-f0a0-4662-8448-a4d9e643c142',
        created_at: '2021-02-17T04:43:32.770206+00:00',
        updated_at: '2021-02-17T04:43:32.770206+00:00',
      },
    ],
  ],
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Gets all buckets'
  const schema = createDefaultSchema(successResponseSchema, {
    summary,
    tags: ['bucket'],
  })

  fastify.get<AuthenticatedRequest>(
    '/',
    {
      schema,
    },
    async (request, response) => {
      // get list of all buckets
      const {
        data: results,
        error,
        status,
      } = await request.postgrest
        .from<Bucket>('buckets')
        .select('id, name, public, owner, created_at, updated_at')

      if (error) {
        request.log.error({ error }, 'error bucket')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      request.log.info({ results }, 'results')

      response.send(results)
    }
  )
}
