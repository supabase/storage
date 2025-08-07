import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { bucketSchema } from '@storage/schemas'
import { ROUTE_OPERATIONS } from '../operations'

const successResponseSchema = {
  type: 'array',
  items: bucketSchema,
  examples: [
    [
      {
        id: 'avatars',
        type: 'STANDARD',
        name: 'avatars',
        owner: '4d56e902-f0a0-4662-8448-a4d9e643c142',
        public: false,
        file_size_limit: 1000000,
        allowed_mime_types: ['image/png', 'image/jpeg'],
        created_at: '2021-02-17T04:43:32.770206+00:00',
        updated_at: '2021-02-17T04:43:32.770206+00:00',
      },
    ],
  ],
}

const requestQuerySchema = {
  type: 'object',
  properties: {
    limit: { type: 'integer', minimum: 1, examples: [10] },
    offset: { type: 'integer', minimum: 0, examples: [0] },
    sortColumn: { type: 'string', enum: ['id', 'name', 'created_at', 'updated_at'] },
    sortOrder: { type: 'string', enum: ['asc', 'desc'] },
    search: { type: 'string', examples: ['my-bucket'] },
  },
} as const

interface GetAllBucketsRequest extends AuthenticatedRequest {
  Querystring: FromSchema<typeof requestQuerySchema>
}

/**
 * Detects user agents that support the type property in bucket list response
 * assumes all can except: storage-py < v0.12.1 (throws fatal error if type property is present)
 * type property added in v0.12.1 -- https://github.com/supabase/storage-py/releases/tag/v0.12.1
 */
function canUserAgentSupportBucketType(userAgent: string): boolean {
  const match = userAgent.match(/supabase-py\/storage3 v(\d+)\.(\d+)\.(\d+)/i)
  if (!match) {
    return true
  }

  const [major, minor, patch] = match.slice(1).map(Number)

  if (major > 0) return true
  if (minor < 12) return false
  if (minor > 12) return true
  return patch >= 1 // version >= v0.12.1
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Gets all buckets'
  const schema = createDefaultSchema(successResponseSchema, {
    querystring: requestQuerySchema,
    summary,
    tags: ['bucket'],
  })

  fastify.get<GetAllBucketsRequest>(
    '/',
    {
      schema,
      config: {
        operation: { type: ROUTE_OPERATIONS.LIST_BUCKET },
      },
    },
    async (request, response) => {
      const includeBucketType = canUserAgentSupportBucketType(request.headers['user-agent'] || '')
      const { limit, offset, sortColumn, sortOrder, search } = request.query
      const results = await request.storage.listBuckets(
        'id, name, public, owner, created_at, updated_at, file_size_limit, allowed_mime_types' +
          (includeBucketType ? ', type' : ''),
        { limit, offset, sortColumn, sortOrder, search }
      )

      return response.send(results)
    }
  )
}
