import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const purgeObjectParamsSchema = {
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
    message: { type: 'string', examples: ['success'] },
  },
}
interface deleteObjectRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof purgeObjectParamsSchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Purge cache for an object'

  const schema = createDefaultSchema(successResponseSchema, {
    params: purgeObjectParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.delete<deleteObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema,
      config: {
        operation: { type: ROUTE_OPERATIONS.PURGE_OBJECT_CACHE },
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const objectName = request.params['*']

      await request.cdnCache.purge({
        objectName,
        bucket: bucketName,
        tenant: request.tenantId,
      })

      return response.status(200).send(createResponse('success', '200'))
    }
  )
}
