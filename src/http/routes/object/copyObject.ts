import { FastifyInstance, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const copyRequestBodySchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', examples: ['avatars'] },
    sourceKey: { type: 'string', examples: ['folder/source.png'] },
    destinationBucket: { type: 'string', examples: ['users'] },
    destinationKey: { type: 'string', examples: ['folder/destination.png'] },
    copyMetadata: { type: 'boolean', examples: [true] },
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
      config: {
        operation: { type: ROUTE_OPERATIONS.COPY_OBJECT },
        resources: (req: FastifyRequest<copyRequestInterface>) => {
          const { sourceKey, destinationKey, bucketId, destinationBucket } = req.body
          return [`${bucketId}/${sourceKey}`, `${destinationBucket || bucketId}/${destinationKey}`]
        },
      },
    },
    async (request, response) => {
      const { sourceKey, destinationKey, bucketId, destinationBucket } = request.body

      const destinationBucketId = destinationBucket || bucketId

      const result = await request.storage.from(bucketId).copyObject({
        sourceKey,
        destinationBucket: destinationBucketId,
        destinationKey,
        owner: request.owner,
        copyMetadata: request.body.copyMetadata ?? true,
      })

      return response.status(result.httpStatusCode ?? 200).send({
        Id: result.destObject.id,
        Key: `${destinationBucketId}/${destinationKey}`,
      })
    }
  )
}
