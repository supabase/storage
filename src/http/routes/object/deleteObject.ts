import { FastifyInstance } from 'fastify'
import { FromSchema, JSONSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const deleteObjectParamsSchema = {
  type: 'object',
  properties: {
    Bucket: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['Bucket', '*'],
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
    '/:Bucket/*',
    {
      schema,
      config: {
        operation: { type: ROUTE_OPERATIONS.DELETE_OBJECT },
      },
    },
    async (request, response) => {
      const { Bucket } = request.params
      const objectName = request.params['*']

      await request.storage.from(Bucket).deleteObject(objectName)

      return response.status(200).send(createResponse('Successfully deleted'))
    }
  )
}
