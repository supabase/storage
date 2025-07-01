import { FastifyInstance } from 'fastify'
import { AuthenticatedRequest } from '../../types'
import { FromSchema } from 'json-schema-to-ts'
import { ERRORS } from '@internal/errors'

const createNamespaceSchema = {
  type: 'object',
  body: {
    type: 'object',
    properties: {
      namespace: { type: 'string', examples: ['namespace'] },
    },
    required: ['namespace'],
  },
  params: {
    type: 'object',
    properties: {
      prefix: { type: 'string', examples: ['prefix'] },
    },
    required: ['prefix'],
  },
  summary: 'Create a namespace',
} as const

const listNamespaceSchema = {
  type: 'object',
  querystring: {
    type: 'object',
    properties: {
      pageToken: { type: 'string' },
      pageSize: { type: 'number' },
      parent: { type: 'string' },
    },
  },
  params: {
    type: 'object',
    properties: {
      prefix: { type: 'string', examples: ['prefix'] },
    },
    required: ['prefix'],
  },
  summary: 'List namespaces',
} as const

const loadNamespaceSchema = {
  type: 'object',
  params: {
    type: 'object',
    properties: {
      prefix: { type: 'string', examples: ['prefix'] },
      namespace: { type: 'string', examples: ['namespace'] },
    },
    required: ['prefix', 'namespace'],
  },
  summary: 'Load a namespace',
} as const

const dropNamespaceSchema = {
  type: 'object',
  params: {
    type: 'object',
    properties: {
      prefix: { type: 'string', examples: ['prefix'] },
      namespace: { type: 'string', examples: ['namespace'] },
    },
    required: ['prefix', 'namespace'],
  },
  summary: 'Create a namespace',
} as const

interface createNamespaceSchemaRequest extends AuthenticatedRequest {
  Body: FromSchema<(typeof createNamespaceSchema)['body']>
  Params: FromSchema<(typeof createNamespaceSchema)['params']>
}

interface listNamespaceSchemaRequest extends AuthenticatedRequest {
  Querystring: FromSchema<(typeof listNamespaceSchema)['querystring']>
  Params: FromSchema<(typeof listNamespaceSchema)['params']>
}

interface dropNamespaceSchemaRequest extends AuthenticatedRequest {
  Params: FromSchema<(typeof dropNamespaceSchema)['params']>
}

interface loadNamespaceSchemaRequest extends AuthenticatedRequest {
  Params: FromSchema<(typeof loadNamespaceSchema)['params']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.post<createNamespaceSchemaRequest>(
    '/:prefix/namespaces',
    {
      schema: createNamespaceSchema,
    },
    async (request, response) => {
      const bucket = await request.storage.findBucket(request.params.prefix, 'name,iceberg_catalog')

      if (!bucket.iceberg_catalog) {
        throw ERRORS.FeatureNotEnabled(request.params.prefix, 'iceberg_catalog')
      }

      const result = await request.icebergCatalog?.createNamespace({
        namespace: [request.body.namespace],
        warehouse: bucket.name,
      })

      return response.send(result)
    }
  )

  fastify.get<listNamespaceSchemaRequest>(
    '/:prefix/namespaces',
    {
      schema: listNamespaceSchema,
    },
    async (request, response) => {
      const bucket = await request.storage.findBucket(request.params.prefix, 'name,iceberg_catalog')

      if (!bucket.iceberg_catalog) {
        throw ERRORS.FeatureNotEnabled(request.params.prefix, 'iceberg_catalog')
      }

      const result = await request.icebergCatalog?.listNamespaces({
        bucketId: bucket.name,
        pageSize: request.query.pageSize || 100,
        pageToken: request.query.pageToken,
        parent: request.query.parent,
      })

      return response.send(result)
    }
  )

  fastify.head<loadNamespaceSchemaRequest>(
    '/:prefix/namespaces/:namespace',
    {
      schema: listNamespaceSchema,
    },
    async (request, response) => {
      const bucket = await request.storage.findBucket(request.params.prefix, 'name,iceberg_catalog')

      if (!bucket.iceberg_catalog) {
        throw ERRORS.FeatureNotEnabled(request.params.prefix, 'iceberg_catalog')
      }

      const result = await request.icebergCatalog?.namespaceExists({
        namespace: request.params.namespace,
      })

      return response.send(result)
    }
  )

  fastify.get<loadNamespaceSchemaRequest>(
    '/:prefix/namespaces/:namespace',
    {
      schema: loadNamespaceSchema,
    },
    async (request, response) => {
      const bucket = await request.storage.findBucket(request.params.prefix, 'name,iceberg_catalog')

      if (!bucket.iceberg_catalog) {
        throw ERRORS.FeatureNotEnabled(request.params.prefix, 'iceberg_catalog')
      }

      const result = await request.icebergCatalog?.loadNamespaceMetadata({
        namespace: request.params.namespace,
      })

      return response.send(result)
    }
  )

  fastify.register(async (f) => {
    f.addContentTypeParser('application/json', { bodyLimit: 0 }, (_request, _payload, done) => {
      done(null, null)
    })

    f.delete<dropNamespaceSchemaRequest>(
      '/:prefix/namespaces/:namespace',
      {
        schema: dropNamespaceSchema,
      },
      async (request, response) => {
        const bucket = await request.storage.findBucket(
          request.params.prefix,
          'name,iceberg_catalog'
        )

        if (!bucket.iceberg_catalog) {
          throw ERRORS.FeatureNotEnabled(request.params.prefix, 'iceberg_catalog')
        }

        await request.icebergCatalog?.dropNamespace({
          namespace: request.params.namespace,
          warehouse: bucket.name,
        })
        return response.status(204).send()
      }
    )
  })
}
