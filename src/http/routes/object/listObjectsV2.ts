import { DBMigration } from '@internal/database/migrations'
import { ErrorCode } from '@internal/errors'
import { FastifyInstance } from 'fastify'
import { FastifyRequest } from 'fastify/types/request'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { sharedErrorResponseSchemas } from '../../schemas/error'
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
    // key only) instead of the usual prefix/range scan - e.g. a sibling key
    // like "folder/cat.png.bak" would no longer also match prefix:
    // "folder/cat.png". Useful for looking up one specific key's version
    // history without any folder-grouping getting in the way.
    exactMatch: { type: 'boolean' },
    limit: { type: 'integer', finite: true, minimum: 1, examples: [10] },
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
    // 'exclude' (default) is today's behavior. 'only' is an "IS" filter, not
    // an "include" toggle - e.g. noncurrentVersions: 'exclude' + deleteMarkers:
    // 'only' answers "what's currently deleted"; noncurrentVersions: 'only' +
    // deleteMarkers: 'only' answers "delete markers that have since been
    // superseded". See migrations/tenant/0062-object-versioning.sql.
    noncurrentVersions: { type: 'string', enum: ['exclude', 'include', 'only'] },
    deleteMarkers: { type: 'string', enum: ['exclude', 'include', 'only'] },
  },
} as const
interface searchRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof searchRequestBodySchema>
  Params: FromSchema<typeof searchRequestParamsSchema>
}
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Search for objects under a prefix'

  fastify.post<searchRequestInterface>(
    '/list-v2/:bucketName',
    {
      schema: {
        body: searchRequestBodySchema,
        params: searchRequestParamsSchema,
        response: sharedErrorResponseSchemas,
        summary,
        tags: ['object'],
      },
      config: {
        operation: ROUTE_OPERATIONS.LIST_OBJECTS_V2,
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
      const latestMigration = request.latestMigration
      if (
        isMultitenant &&
        latestMigration &&
        DBMigration[latestMigration] < DBMigration['search-v2']
      ) {
        return response.status(400).send({
          statusCode: '400',
          error: 'FeatureNotEnabled',
          message: 'This feature is not available for your tenant',
          code: ErrorCode.FeatureNotEnabled,
        })
      }

      const { bucketName } = request.params
      const {
        limit,
        with_delimiter,
        cursor,
        prefix,
        sortBy,
        noncurrentVersions,
        deleteMarkers,
        exactMatch,
      } = request.body

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

      const results = await request.storage.from(bucketName).listObjectsV2({
        prefix,
        delimiter: with_delimiter ? '/' : undefined,
        maxKeys: limit,
        cursor,
        sortBy,
        noncurrentVersions,
        deleteMarkers,
        exactMatch,
      })

      return response.status(200).send(results)
    }
  )
}
