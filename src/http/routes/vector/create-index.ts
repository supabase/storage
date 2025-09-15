import { FastifyInstance } from 'fastify'
import { AuthenticatedRequest } from '../../types'
import { FromSchema } from 'json-schema-to-ts'
import { ERRORS } from '@internal/errors'
import { ROUTE_OPERATIONS } from '../operations'

const createVectorIndex = {
  type: 'object',
  body: {
    type: 'object',
    properties: {
      dataType: { type: 'string', enum: ['float32'] },
      dimension: { type: 'number' },
      distanceMetric: { type: 'string', enum: ['cosine', 'euclidean'] },
      indexName: { type: 'string' },
      metadataConfiguration: {
        type: 'object',
        required: ['nonFilterableMetadataKeys'],
        properties: {
          nonFilterableMetadataKeys: {
            type: 'array',
            items: { type: 'string' },
          },
        },
      },
      vectorBucketName: { type: 'string' },
    },
    required: ['dataType', 'dimension', 'distanceMetric', 'indexName', 'vectorBucketName'],
  },
  summary: 'Create a vector index',
} as const

interface createVectorIndexRequest extends AuthenticatedRequest {
  Body: FromSchema<(typeof createVectorIndex)['body']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.post<createVectorIndexRequest>(
    '/CreateIndex',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.CREATE_VECTOR_INDEX },
      },
      schema: {
        ...createVectorIndex,
        tags: ['vector'],
      },
    },
    async (request, response) => {
      if (!request.s3Vector) {
        throw ERRORS.FeatureNotEnabled('vectorStore', 'Vector service not configured')
      }

      await request.s3Vector.createVectorIndex(request.body)

      return response.send()
    }
  )
}
