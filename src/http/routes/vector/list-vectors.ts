import { ERRORS } from '@internal/errors'
import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const listVectors = {
  type: 'object',
  body: {
    type: 'object',
    properties: {
      vectorBucketName: { type: 'string' },
      indexArn: { type: 'string' },
      indexName: {
        type: 'string',
        minLength: 3,
        maxLength: 45,
        pattern: '^[a-z0-9](?:[a-z0-9.-]{1,61})?[a-z0-9]$',
        description:
          '3-63 chars, lowercase letters, numbers, hyphens, dots; must start/end with letter or number. Must be unique within the vector bucket.',
      },
      maxResults: { type: 'number', minimum: 1, maximum: 500 },
      nextToken: { type: 'string' },
      returnData: { type: 'boolean' },
      returnMetadata: { type: 'boolean' },
      segmentCount: { type: 'number', minimum: 1, maximum: 16 },
      segmentIndex: { type: 'number', minimum: 0, maximum: 15 },
    },
    required: ['vectorBucketName', 'indexName'],
  },
  summary: 'List vectors in a vector index',
} as const

interface listVectorsRequest extends AuthenticatedRequest {
  Body: FromSchema<(typeof listVectors)['body']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.post<listVectorsRequest>(
    '/ListVectors',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.LIST_VECTORS },
      },
      schema: {
        ...listVectors,
        tags: ['vector'],
      },
    },
    async (request, response) => {
      if (!request.s3Vector) {
        throw ERRORS.FeatureNotEnabled('vectorStore', 'Vector service not configured')
      }

      const indexResult = await request.s3Vector.listVectors(request.body)

      return response.send(indexResult)
    }
  )
}
