import { ERRORS } from '@internal/errors'
import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const getVectors = {
  type: 'object',
  body: {
    type: 'object',
    properties: {
      indexName: { type: 'string' },
      keys: { type: 'array', items: { type: 'string' } },
      returnData: { type: 'boolean', default: false },
      returnMetadata: { type: 'boolean', default: false },
      vectorBucketName: { type: 'string' },
    },
    required: ['indexName', 'keys', 'vectorBucketName'],
  },
  summary: 'Returns vector attributes',
} as const

interface getVectorsRequest extends AuthenticatedRequest {
  Body: FromSchema<(typeof getVectors)['body']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.post<getVectorsRequest>(
    '/GetVectors',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.GET_VECTORS },
      },
      schema: {
        ...getVectors,
        tags: ['vector'],
      },
    },
    async (request, response) => {
      if (!request.s3Vector) {
        throw ERRORS.FeatureNotEnabled('vectorStore', 'Vector service not configured')
      }

      const indexResult = await request.s3Vector.getVectors(request.body)

      return response.send(indexResult)
    }
  )
}
