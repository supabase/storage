import { FastifyInstance, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'

const moveObjectsBodySchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', examples: ['avatars'] },
    sourceKey: { type: 'string', examples: ['folder/cat.png'] },
    destinationKey: { type: 'string', examples: ['folder/newcat.png'] },
  },
  required: ['bucketId', 'sourceKey', 'destinationKey'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string', examples: ['Successfully moved'] },
  },
  required: ['message'],
}
interface moveObjectRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof moveObjectsBodySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Moves an object'

  const schema = createDefaultSchema(successResponseSchema, {
    body: moveObjectsBodySchema,
    summary,
    tags: ['object'],
  })

  fastify.post<moveObjectRequestInterface>(
    '/move',
    {
      schema,
      config: {
        getParentBucketId: (request: FastifyRequest<moveObjectRequestInterface>) => {
          return request.body.bucketId
        },
      },
    },
    async (request, response) => {
      const { destinationKey, sourceKey } = request.body

      await request.storage
        .from(request.bucket)
        .moveObject(sourceKey, destinationKey, request.owner)

      return response.status(200).send(createResponse('Successfully moved'))
    }
  )
}
