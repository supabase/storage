import { DBMigration } from '@internal/database/migrations'
import { ErrorCode } from '@internal/errors'
import { objectSchema } from '@storage/schemas'
import { FastifyInstance } from 'fastify'
import { FastifyRequest } from 'fastify/types/request'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { createDefaultSchema } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

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
    // When true, prefix is matched with a plain equality check (this exact
    // key only) instead of the usual prefix/range scan.
    exactMatch: { type: 'boolean' },
    limit: { type: 'integer', finite: true, minimum: 1, examples: [10] },
    offset: { type: 'integer', finite: true, minimum: 0, examples: [0] },
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
    // 'exclude' (default) is today's behavior. 'only' is an "IS" filter, not
    // an "include" toggle - see migrations/tenant/0062-object-versioning.sql.
    noncurrentVersions: { type: 'string', enum: ['exclude', 'include', 'only'] },
    deleteMarkers: { type: 'string', enum: ['exclude', 'include', 'only'] },
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
        operation: ROUTE_OPERATIONS.LIST_OBJECTS,
        logMetadata: (req: FastifyRequest<searchRequestInterface>) => ({
          prefix: req.body.prefix,
          limit: req.body.limit,
          offset: req.body.offset,
          sortBy: req.body.sortBy,
        }),
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const {
        limit,
        offset,
        sortBy,
        search,
        prefix,
        noncurrentVersions,
        deleteMarkers,
        exactMatch,
      } = request.body

      const latestMigration = request.latestMigration
      if (
        isMultitenant &&
        latestMigration &&
        (noncurrentVersions !== undefined || deleteMarkers !== undefined) &&
        DBMigration[latestMigration] < DBMigration['object-versioning']
      ) {
        return response.status(400).send({
          statusCode: '400',
          error: 'FeatureNotEnabled',
          message: 'This feature is not available for your tenant',
          code: ErrorCode.FeatureNotEnabled,
        })
      }

      const results = await request.storage.from(bucketName).searchObjects(prefix, {
        limit,
        offset,
        search,
        sortBy: {
          column: sortBy?.column,
          order: sortBy?.order,
        },
        noncurrentVersions,
        deleteMarkers,
        exactMatch,
      })

      return response.status(200).send(results)
    }
  )
}
