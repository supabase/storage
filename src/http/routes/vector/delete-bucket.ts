import { ERRORS } from '@internal/errors'
import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const deleteVectorBucket = {
  type: 'object',
  body: {
    type: 'object',
    properties: {
      vectorBucketName: { type: 'string' },
    },
    required: ['vectorBucketName'],
  },
  summary: 'Create a vector bucket',
} as const

interface deleteVectorIndexRequest extends AuthenticatedRequest {
  Body: FromSchema<(typeof deleteVectorBucket)['body']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.post<deleteVectorIndexRequest>(
    '/DeleteVectorBucket',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.DELETE_VECTOR_BUCKET },
      },
      schema: {
        ...deleteVectorBucket,
        tags: ['vector'],
      },
    },
    async (request, response) => {
      if (!request.s3Vector) {
        throw ERRORS.FeatureNotEnabled('vectorStore', 'Vector service not configured')
      }

      await request.s3Vector.deleteBucket(request.body.vectorBucketName)

      return response.send()
    }
  )
}
