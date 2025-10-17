import { FastifyInstance } from 'fastify'
import { AuthenticatedRequest } from '../../types'
import { FromSchema } from 'json-schema-to-ts'
import { ERRORS } from '@internal/errors'
import { ROUTE_OPERATIONS } from '../operations'

const getVectorBucket = {
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

interface getVectorBucketRequest extends AuthenticatedRequest {
  Body: FromSchema<(typeof getVectorBucket)['body']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.post<getVectorBucketRequest>(
    '/GetVectorBucket',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.GET_VECTOR_BUCKET },
      },
      schema: {
        ...getVectorBucket,
        tags: ['vector'],
      },
    },
    async (request, response) => {
      if (!request.s3Vector) {
        throw ERRORS.FeatureNotEnabled('vectorStore', 'Vector service not configured')
      }

      const bucketResult = await request.s3Vector.getBucket(request.body)

      return response.send(bucketResult)
    }
  )
}
