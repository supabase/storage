import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest, Bucket } from '../../types/types'
import { transformPostgrestError } from '../../utils'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'

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

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
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

      const { error, status } = await request.postgrest
        .from<Bucket>('buckets')
        .update({
          public: isPublic,
        })
        .match({ id: bucketId })
        .single()

      if (error) {
        request.log.error({ error }, 'error updating bucket')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      return response.status(200).send(createResponse('Successfully updated'))
    }
  )
}
