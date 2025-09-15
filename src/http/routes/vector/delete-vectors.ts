import { FastifyInstance } from 'fastify'
import { AuthenticatedRequest } from '../../types'
import { FromSchema } from 'json-schema-to-ts'
import { ERRORS } from '@internal/errors'
import { ROUTE_OPERATIONS } from '../operations'

const deleteVector = {
  body: {
    type: 'object',
    properties: {
      vectorBucketName: { type: 'string' },
      indexName: { type: 'string' },
      keys: { type: 'array', items: { type: 'string' } },
    },
    required: ['vectorBucketName', 'indexName', 'keys'],
  },
  summary: 'Delete vectors from an index',
} as const

interface deleteVectorRequest extends AuthenticatedRequest {
  Body: FromSchema<(typeof deleteVector)['body']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.post<deleteVectorRequest>(
    '/DeleteVectors',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.DELETE_VECTORS },
      },
      schema: {
        ...deleteVector,
        tags: ['vector'],
      },
    },
    async (request, response) => {
      if (!request.s3Vector) {
        throw ERRORS.FeatureNotEnabled('vectorStore', 'Vector service not configured')
      }

      await request.s3Vector.deleteVectors({
        vectorBucketName: request.body.vectorBucketName,
        indexName: request.body.indexName,
        keys: request.body.keys,
      })

      return response.send()
    }
  )
}
