import { FastifyInstance } from 'fastify'
import { createDefaultSchema } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'
import { bucketSchema } from '../../../storage/schemas'

const successResponseSchema = {
  type: 'array',
  items: bucketSchema,
  examples: [
    [
      {
        id: 'avatars',
        name: 'avatars',
        owner: '4d56e902-f0a0-4662-8448-a4d9e643c142',
        public: false,
        file_size_limit: 1000000,
        allowed_mime_types: ['image/png', 'image/jpeg'],
        created_at: '2021-02-17T04:43:32.770206+00:00',
        updated_at: '2021-02-17T04:43:32.770206+00:00',
      },
    ],
  ],
}

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
      const results = await request.storage.listBuckets(
        'id, name, public, owner, created_at, updated_at, file_size_limit, allowed_mime_types'
      )

      response.send(results)
    }
  )
}
