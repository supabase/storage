import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'

const createBucketBodySchema = {
  type: 'object',
  properties: {
    name: { type: 'string', examples: ['avatars'] },
    id: { type: 'string', examples: ['avatars'] },
    public: { type: 'boolean', examples: [false] },
  },
  required: ['name'],
} as const

const successResponseSchema = {
  type: 'object',
  properties: {
    name: { type: 'string', examples: ['avatars'] },
  },
  required: ['name'],
}
interface createBucketRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof createBucketBodySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Create a bucket'
  const schema = createDefaultSchema(successResponseSchema, {
    body: createBucketBodySchema,
    summary,
    tags: ['bucket'],
  })
  fastify.post<createBucketRequestInterface>(
    '/',
    {
      schema,
    },
    async (request, response) => {
      const owner = request.owner

      const { name: bucketName, public: isPublic, id } = request.body

      const bucket = await request.storage.createBucket({
        id: id ?? bucketName,
        name: bucketName,
        owner,
        public: isPublic ?? false,
      })

      request.log.info({ results: bucket }, 'results')

      return response.status(200).send({
        name: bucketName,
      })
    }
  )
}
