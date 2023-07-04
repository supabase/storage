import { FastifyInstance, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'

const deleteObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string', examples: ['Successfully deleted'] },
  },
}
interface deleteObjectRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof deleteObjectParamsSchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Delete an object'

  const schema = createDefaultSchema(successResponseSchema, {
    params: deleteObjectParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.delete<deleteObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema,
      config: {
        getParentBucketId: (request: FastifyRequest<deleteObjectRequestInterface>) => {
          return request.params.bucketName
        },
      },
    },
    async (request, response) => {
      const objectName = request.params['*']

      await request.storage.from(request.bucket).deleteObject(objectName)

      return response.status(200).send(createResponse('Successfully deleted'))
    }
  )
}
