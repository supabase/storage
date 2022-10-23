import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'

const updateBucketBodySchema = {
  type: 'object',
  properties: {
    public: { type: 'boolean', examples: [false] },
  },
} as const
const updateBucketParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketId'],
} as const

const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string', examples: ['Successfully updated'] },
  },
  required: ['message'],
}
interface updateBucketRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof updateBucketBodySchema>
  Params: FromSchema<typeof updateBucketParamsSchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Update properties of a bucket'
  const schema = createDefaultSchema(successResponseSchema, {
    body: updateBucketBodySchema,
    summary,
    tags: ['bucket'],
  })
  fastify.put<updateBucketRequestInterface>(
    '/:bucketId',
    {
      schema,
    },
    async (request, response) => {
      const { bucketId } = request.params

      const { public: isPublic } = request.body

      await request.storage.updateBucket(bucketId, isPublic)

      return response.status(200).send(createResponse('Successfully updated'))
    }
  )
}
