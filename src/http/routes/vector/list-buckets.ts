import { FastifyInstance } from 'fastify'
import { AuthenticatedRequest } from '../../types'
import { FromSchema } from 'json-schema-to-ts'
import { ERRORS } from '@internal/errors'
import { ROUTE_OPERATIONS } from '../operations'

const listBucket = {
  type: 'object',
  body: {
    type: 'object',
    properties: {
      maxResults: { type: 'number', minimum: 1, maximum: 500, default: 500 },
      nextToken: { type: 'string' },
      prefix: { type: 'string' },
    },
  },
  summary: 'List vector buckets',
} as const

interface listBucketRequest extends AuthenticatedRequest {
  Body: FromSchema<(typeof listBucket)['body']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.post<listBucketRequest>(
    '/ListVectorBuckets',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.LIST_VECTOR_BUCKETS },
      },
      schema: {
        ...listBucket,
        tags: ['vector'],
      },
    },
    async (request, response) => {
      if (!request.s3Vector) {
        throw ERRORS.FeatureNotEnabled('vectorStore', 'Vector service not configured')
      }

      const listBucketsResult = await request.s3Vector.listBuckets(request.body)

      return response.send(listBucketsResult)
    }
  )
}
