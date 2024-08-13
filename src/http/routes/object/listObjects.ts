import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { objectSchema } from '@storage/schemas'
import { ROUTE_OPERATIONS } from '../operations'

const searchRequestParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string' },
  },
  required: ['bucketName'],
} as const
const searchRequestBodySchema = {
  type: 'object',
  properties: {
    prefix: { type: 'string', examples: ['folder/subfolder'] },
    limit: { type: 'integer', minimum: 1, examples: [10] },
    offset: { type: 'integer', minimum: 0, examples: [0] },
    sortBy: {
      type: 'object',
      properties: {
        column: { type: 'string', enum: ['name', 'updated_at', 'created_at', 'last_accessed_at'] },
        order: { type: 'string', enum: ['asc', 'desc'] },
      },
      required: ['column'],
    },
    search: {
      type: 'string',
    },
  },
  required: ['prefix'],
} as const
const successResponseSchema = {
  type: 'array',
  items: objectSchema,
}
interface searchRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof searchRequestBodySchema>
  Params: FromSchema<typeof searchRequestParamsSchema>
}
// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Search for objects under a prefix'

  const schema = createDefaultSchema(successResponseSchema, {
    body: searchRequestBodySchema,
    params: searchRequestParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.post<searchRequestInterface>(
    '/list/:bucketName',
    {
      schema,
      config: {
        operation: { type: ROUTE_OPERATIONS.LIST_OBJECTS },
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const { limit, offset, sortBy, search, prefix } = request.body

      const results = await request.storage.from(bucketName).searchObjects(prefix, {
        limit,
        offset,
        search,
        sortBy: {
          column: sortBy?.column,
          order: sortBy?.order,
        },
      })

      return response.status(200).send(results)
    }
  )
}
