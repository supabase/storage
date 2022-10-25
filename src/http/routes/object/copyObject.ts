import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'

const copyRequestBodySchema = {
  type: 'object',
  properties: {
    sourceKey: { type: 'string', examples: ['folder/source.png'] },
    bucketId: { type: 'string', examples: ['avatars'] },
    destinationKey: { type: 'string', examples: ['folder/destination.png'] },
  },
  required: ['sourceKey', 'bucketId', 'destinationKey'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    Key: { type: 'string', examples: ['folder/destination.png'] },
  },
  required: ['Key'],
}
interface copyRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof copyRequestBodySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Copies an object'

  const schema = createDefaultSchema(successResponseSchema, {
    body: copyRequestBodySchema,
    summary,
    tags: ['object'],
  })

  fastify.post<copyRequestInterface>(
    '/copy',
    {
      schema,
    },
    async (request, response) => {
      const { sourceKey, destinationKey, bucketId } = request.body
      request.log.info(
        'sourceKey is %s and bucketName is %s and destinationKey is %s',
        sourceKey,
        bucketId,
        destinationKey
      )

      const result = await request.storage
        .from(bucketId)
        .copyObject(sourceKey, destinationKey, request.owner)

      return response.status(result.httpStatusCode ?? 200).send({
        Key: `${bucketId}/${destinationKey}`,
      })
    }
  )
}
