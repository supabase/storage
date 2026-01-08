import { FastifyInstance } from 'fastify'
import { AuthenticatedRequest } from '../../types'
import { FromSchema } from 'json-schema-to-ts'
import { ERRORS } from '@internal/errors'
import { ROUTE_OPERATIONS } from '../operations'

const putVector = {
  body: {
    type: 'object',
    properties: {
      vectorBucketName: { type: 'string' },
      indexName: {
        type: 'string',
        minLength: 3,
        maxLength: 45,
        pattern: '^[a-z0-9](?:[a-z0-9.-]{1,61})?[a-z0-9]$',
        description:
          '3-63 chars, lowercase letters, numbers, hyphens, dots; must start/end with letter or number. Must be unique within the vector bucket.',
      },
      vectors: {
        type: 'array',
        minItems: 1,
        maxItems: 500,
        items: {
          type: 'object',
          properties: {
            data: {
              type: 'object',
              properties: {
                float32: { type: 'array', items: { type: 'number' } },
              },
              required: ['float32'],
            },
            metadata: {
              type: 'object',
              additionalProperties: {
                oneOf: [{ type: 'string' }, { type: 'boolean' }, { type: 'number' }],
              },
            },
            key: { type: 'string' },
          },
          required: ['data'],
        },
      },
    },
    required: ['vectorBucketName', 'indexName', 'vectors'],
  },
  summary: 'Put vectors into an index',
} as const

interface putVectorRequest extends AuthenticatedRequest {
  Body: FromSchema<(typeof putVector)['body']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.post<putVectorRequest>(
    '/PutVectors',
    {
      bodyLimit: 20 * 1024 * 1024, // 20 MB
      config: {
        operation: { type: ROUTE_OPERATIONS.PUT_VECTORS },
      },
      schema: {
        ...putVector,
        tags: ['vector'],
      },
    },
    async (request, response) => {
      if (!request.s3Vector) {
        throw ERRORS.FeatureNotEnabled('vectorStore', 'Vector service not configured')
      }

      const indexResult = await request.s3Vector.putVectors({
        vectorBucketName: request.body.vectorBucketName,
        indexName: request.body.indexName,
        vectors: request.body.vectors.map((v) => {
          return {
            ...v,
            key: v.key || undefined,
          }
        }),
      })

      return response.send(indexResult)
    }
  )
}
