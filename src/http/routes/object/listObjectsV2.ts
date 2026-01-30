import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'
import { getConfig } from '../../../config'
import { getTenantConfig } from '@internal/database'
import { DBMigration } from '@internal/database/migrations'
import { FastifyRequest } from 'fastify/types/request'

const { isMultitenant } = getConfig()

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
    cursor: { type: 'string' },
    with_delimiter: { type: 'boolean' },
    sortBy: {
      type: 'object',
      properties: {
        column: { type: 'string', enum: ['name', 'updated_at', 'created_at'] },
        order: { type: 'string', enum: ['asc', 'desc'] },
      },
      required: ['column'],
    },
  },
} as const
interface searchRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof searchRequestBodySchema>
  Params: FromSchema<typeof searchRequestParamsSchema>
}
// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Search for objects under a prefix'

  fastify.post<searchRequestInterface>(
    '/list-v2/:bucketName',
    {
      schema: {
        body: searchRequestBodySchema,
        params: searchRequestParamsSchema,
        summary,
        tags: ['object'],
      },
      config: {
        operation: { type: ROUTE_OPERATIONS.LIST_OBJECTS_V2 },
        logMetadata: (req: FastifyRequest<searchRequestInterface>) => ({
          prefix: req.body.prefix,
          limit: req.body.limit,
          cursor: req.body.cursor,
          sortBy: req.body.sortBy,
          with_delimiter: req.body.with_delimiter,
        }),
      },
    },
    async (request, response) => {
      if (isMultitenant) {
        const { migrationVersion } = await getTenantConfig(request.tenantId)
        if (migrationVersion && DBMigration[migrationVersion] < DBMigration['search-v2']) {
          return response.status(400).send({
            message: 'This feature is not available for your tenant',
          })
        }
      }

      const { bucketName } = request.params
      const { limit, with_delimiter, cursor, prefix, sortBy } = request.body

      const results = await request.storage.from(bucketName).listObjectsV2({
        prefix,
        delimiter: with_delimiter ? '/' : undefined,
        maxKeys: limit,
        cursor,
        sortBy,
      })

      return response.status(200).send(results)
    }
  )
}
