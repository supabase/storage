import { FastifyInstance } from 'fastify'
import { AuthenticatedRequest } from '../../types'
import { FromSchema } from 'json-schema-to-ts'
import { ERRORS } from '@internal/errors'
import { ROUTE_OPERATIONS } from '../operations'

const createVectorBucket = {
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

interface createVectorIndexRequest extends AuthenticatedRequest {
  Body: FromSchema<(typeof createVectorBucket)['body']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.post<createVectorIndexRequest>(
    '/CreateVectorBucket',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.CREATE_VECTOR_BUCKET },
      },
      schema: {
        ...createVectorBucket,
        tags: ['vector'],
      },
    },
    async (request, response) => {
      if (!request.s3Vector) {
        throw ERRORS.FeatureNotEnabled('vectorStore', 'Vector service not configured')
      }

      await request.s3Vector.createBucket(request.body.vectorBucketName)

      return response.send()
    }
  )
}
