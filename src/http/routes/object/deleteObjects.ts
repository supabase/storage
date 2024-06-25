import { FastifyInstance, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { objectSchema } from '@storage/schemas/object'
import { ROUTE_OPERATIONS } from '../operations'
const deleteObjectsParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketName'],
} as const
const deleteObjectsBodySchema = {
  type: 'object',
  properties: {
    prefixes: {
      type: 'array',
      items: { type: 'string' },
      minItems: 1,
      examples: [['folder/cat.png', 'folder/morecats.png']],
    },
  },
  required: ['prefixes'],
} as const
const successResponseSchema = {
  type: 'array',
  items: objectSchema,
}
interface deleteObjectsInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof deleteObjectsParamsSchema>
  Body: FromSchema<typeof deleteObjectsBodySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Delete multiple objects'

  const schema = createDefaultSchema(successResponseSchema, {
    body: deleteObjectsBodySchema,
    params: deleteObjectsParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.delete<deleteObjectsInterface>(
    '/:bucketName',
    {
      schema,
      config: {
        operation: { type: ROUTE_OPERATIONS.DELETE_OBJECTS },
        resources: (req: FastifyRequest<deleteObjectsInterface>) => {
          const { prefixes } = req.body
          return prefixes.map((prefix) => `${req.params.bucketName}/${prefix}`)
        },
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const prefixes = request.body['prefixes']

      const results = await request.storage.from(bucketName).deleteObjects(prefixes)

      return response.status(200).send(results)
    }
  )
}
