import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const deleteBucketParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketId'],
} as const

const createBucketBodySchema = {
  type: 'object',
  properties: {
    name: { type: 'string', examples: ['avatars'] },
  },
  required: ['name'],
} as const

const listBucketsQuerySchema = {
  type: 'object',
  properties: {
    limit: { type: 'integer', minimum: 1, examples: [10] },
    offset: { type: 'integer', minimum: 0, examples: [0] },
    sortColumn: { type: 'string', enum: ['id', 'name', 'created_at', 'updated_at'] },
    sortOrder: { type: 'string', enum: ['asc', 'desc'] },
    search: { type: 'string', examples: ['my-bucket'] },
  },
} as const

const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string', examples: ['Successfully deleted'] },
  },
}

interface deleteBucketRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof deleteBucketParamsSchema>
}

interface createBucketRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof createBucketBodySchema>
}

interface listBucketRequestInterface extends AuthenticatedRequest {
  Querystring: FromSchema<typeof listBucketsQuerySchema>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.delete<deleteBucketRequestInterface>(
    '/bucket/:bucketId',
    {
      schema: createDefaultSchema(successResponseSchema, {
        params: deleteBucketParamsSchema,
        summary: 'Delete an analytics bucket',
        tags: ['bucket'],
      }),
      config: {
        operation: { type: ROUTE_OPERATIONS.DELETE_BUCKET },
      },
    },
    async (request, response) => {
      const { bucketId } = request.params
      await request.storage.deleteIcebergBucket(bucketId)

      return response.status(200).send(createResponse('Successfully deleted'))
    }
  )

  fastify.post<createBucketRequestInterface>(
    '/bucket',
    {
      schema: {
        body: createBucketBodySchema,
        summary: 'Create an analytics bucket',
        tags: ['bucket'],
      },
      config: {
        operation: { type: ROUTE_OPERATIONS.CREATE_BUCKET },
      },
    },
    async (request, response) => {
      const { name } = request.body
      const bucket = await request.storage.createIcebergBucket({
        id: name,
      })

      return response.status(200).send(bucket)
    }
  )

  fastify.get<listBucketRequestInterface>(
    '/bucket',
    {
      schema: {
        querystring: listBucketsQuerySchema,
        summary: 'List analytics buckets',
        tags: ['bucket'],
      },
      config: {
        operation: { type: ROUTE_OPERATIONS.LIST_BUCKET },
      },
    },
    async (request, response) => {
      const query = request.query

      const bucket = await request.storage.listIcebergBuckets('id,created_at,updated_at', {
        limit: query.limit,
        offset: query.offset,
        sortColumn: query.sortColumn,
        sortOrder: query.sortOrder,
        search: query.search,
      })

      return response.status(200).send(bucket)
    }
  )
}
