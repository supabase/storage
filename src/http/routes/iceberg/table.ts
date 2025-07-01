import { FastifyInstance } from 'fastify'
import { AuthenticatedRequest } from '../../types'
import { FromSchema } from 'json-schema-to-ts'
import { ERRORS } from '@internal/errors'
import { CreateTableRequest } from '@storage/protocols/iceberg/catalog/rest-base-proxy'

const createTableSchema = {
  body: {
    type: 'object',
    required: ['name', 'schema'],
    properties: {
      name: { type: 'string' },
      location: { type: 'string', format: 'uri', nullable: true },

      schema: {
        allOf: [
          {
            type: 'object',
            required: ['type', 'fields'],
            properties: {
              type: { type: 'string', enum: ['struct'] },
              fields: {
                type: 'array',
                items: {
                  type: 'object',
                  required: ['id', 'name', 'type', 'required'],
                  properties: {
                    id: { type: 'integer' },
                    name: { type: 'string' },
                    // A field’s type can be a primitive or any nested container
                    type: {
                      oneOf: [
                        { type: 'string' }, // PrimitiveType
                        {
                          type: 'object', // StructType
                          required: ['type', 'fields'],
                          properties: {
                            type: { type: 'string', enum: ['struct'] },
                            fields: {
                              type: 'array',
                              items: { $comment: 'recurse nested StructField definitions here' },
                            },
                          },
                        },
                        {
                          type: 'object', // ListType
                          required: ['type', 'element-id', 'element', 'element-required'],
                          properties: {
                            type: { type: 'string', enum: ['list'] },
                            'element-id': { type: 'integer' },
                            element: { $comment: 'Type object (recurse)' },
                            'element-required': { type: 'boolean' },
                          },
                        },
                        {
                          type: 'object', // MapType
                          required: [
                            'type',
                            'key-id',
                            'key',
                            'value-id',
                            'value',
                            'value-required',
                          ],
                          properties: {
                            type: { type: 'string', enum: ['map'] },
                            'key-id': { type: 'integer' },
                            key: { $comment: 'Type object (recurse)' },
                            'value-id': { type: 'integer' },
                            value: { $comment: 'Type object (recurse)' },
                            'value-required': { type: 'boolean' },
                          },
                        },
                      ],
                    },
                    required: { type: 'boolean' },
                    doc: { type: 'string' },
                  },
                },
              },
            },
          },
          {
            type: 'object',
            properties: {
              'schema-id': { type: 'integer', readOnly: true },
              'identifier-field-ids': {
                type: 'array',
                items: { type: 'integer' },
              },
            },
          },
        ],
      },

      spec: {
        type: 'object',
        required: ['fields'],
        properties: {
          'spec-id': { type: 'integer', readOnly: true },
          fields: {
            type: 'array',
            items: {
              type: 'object',
              required: ['source-id', 'transform', 'name'],
              properties: {
                'field-id': { type: 'integer' },
                'source-id': { type: 'integer' },
                name: { type: 'string' },
                transform: { type: 'string' },
              },
            },
          },
        },
      },
      properties: {
        type: 'object',
        additionalProperties: { type: 'string' },
      },
      'stage-create': { type: 'boolean', default: false },
      'write-order': {
        type: 'object',
        nullable: true,
        required: ['fields'],
        properties: {
          'order-id': { type: 'integer', readOnly: true },
          fields: {
            type: 'array',
            items: {
              type: 'object',
              required: ['source-id', 'transform', 'direction', 'null-order'],
              properties: {
                'source-id': { type: 'integer' },
                transform: { type: 'string' },
                direction: { type: 'string', enum: ['asc', 'desc'] },
                'null-order': { type: 'string', enum: ['nulls-first', 'nulls-last'] },
              },
            },
          },
        },
      },
    },
  },
  params: {
    type: 'object',
    properties: {
      prefix: { type: 'string', examples: ['prefix'] },
      namespace: { type: 'string', examples: ['prefix'] },
    },
    required: ['prefix', 'namespace'],
  },
  summary: 'Create a table in the given namespace',
} as const

const listTableSchema = {
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
      namespace: { type: 'string', examples: ['namespace'] },
    },
    required: ['prefix', 'namespace'],
  },
  summary: 'Create a namespace',
} as const

const loadTableSchema = {
  type: 'object',
  params: {
    type: 'object',
    properties: {
      prefix: { type: 'string', examples: ['prefix'] },
      namespace: { type: 'string', examples: ['namespace'] },
      table: { type: 'string', examples: ['table'] },
    },
    required: ['prefix', 'namespace', 'table'],
  },
  summary: 'Load an Iceberg Table',
} as const

const dropTableSchema = {
  type: 'object',
  params: {
    type: 'object',
    properties: {
      prefix: { type: 'string', examples: ['prefix'] },
      namespace: { type: 'string', examples: ['namespace'] },
      table: { type: 'string', examples: ['table'] },
    },
    required: ['prefix', 'namespace', 'table'],
  },
  summary: 'Create a namespace',
} as const

const commitTransactionSchema = {
  type: 'object',
  params: {
    type: 'object',
    properties: {
      prefix: { type: 'string', examples: ['prefix'] },
      namespace: { type: 'string', examples: ['namespace'] },
      table: { type: 'string', examples: ['table'] },
    },
    required: ['prefix', 'namespace', 'table'],
  },
  body: {
    type: 'object',
    description: 'Commit updates to multiple tables in an atomic operation',
    properties: {
      requirements: {
        type: 'array',
        description: 'Assertions to validate before applying updates',
        items: {
          type: 'object',
          description: 'A requirement assertion',
          properties: {
            name: {
              type: 'string',
              description: 'Name of the requirement (e.g. assert-ref-snapshot-id)',
              examples: ['assert-ref-snapshot-id'],
            },
            args: {
              type: 'object',
              description: 'Arguments for the requirement',
              additionalProperties: true,
            },
          },
        },
      },
      updates: {
        type: 'array',
        description: 'Metadata updates to apply to the table',
        items: {
          type: 'object',
          description: 'A single update operation',
          properties: {
            name: {
              type: 'string',
              description: 'Name of the update operation (e.g. add-column)',
              examples: ['add-column'],
            },
            args: {
              type: 'object',
              description: 'Arguments for the update operation',
              additionalProperties: true,
            },
          },
        },
      },
    },
    required: ['updates', 'requirements'],
  },
  summary: 'Commit updates to multiple tables in an atomic operation',
} as const

interface createTableSchemaRequest extends AuthenticatedRequest {
  Body: FromSchema<(typeof createTableSchema)['body']>
  Params: FromSchema<(typeof createTableSchema)['params']>
}

interface listTableSchemaRequest extends AuthenticatedRequest {
  Querystring: FromSchema<(typeof listTableSchema)['querystring']>
  Params: FromSchema<(typeof listTableSchema)['params']>
}

interface dropTableSchemaRequest extends AuthenticatedRequest {
  Params: FromSchema<(typeof dropTableSchema)['params']>
}

interface loadTableRequest extends AuthenticatedRequest {
  Params: FromSchema<(typeof loadTableSchema)['params']>
}

interface commitTableRequest extends AuthenticatedRequest {
  Params: FromSchema<(typeof commitTransactionSchema)['params']>
  Body: FromSchema<(typeof commitTransactionSchema)['body']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.post<createTableSchemaRequest>(
    '/:prefix/namespaces/:namespace/tables',
    {
      schema: createTableSchema,
    },
    async (request, response) => {
      const bucket = await request.storage.findBucket(request.params.prefix, 'name,iceberg_catalog')

      if (!bucket.iceberg_catalog || !request.icebergCatalog) {
        throw ERRORS.FeatureNotEnabled(request.params.prefix, 'iceberg_catalog')
      }

      const result = await request.icebergCatalog?.createTable({
        ...(request.body as unknown as CreateTableRequest),
        warehouse: bucket.name,
        namespace: request.params.namespace,
      })

      return response.send(result)
    }
  )

  fastify.get<listTableSchemaRequest>(
    '/:prefix/namespaces/:namespace/tables',
    {
      schema: listTableSchema,
    },
    async (request, response) => {
      const bucket = await request.storage.findBucket(request.params.prefix, 'name,iceberg_catalog')

      if (!bucket.iceberg_catalog) {
        throw ERRORS.FeatureNotEnabled(request.params.prefix, 'iceberg_catalog')
      }

      const result = await request.icebergCatalog?.listTables({
        warehouse: bucket.name,
        namespace: request.params.namespace,
        pageSize: request.query.pageSize || 100,
        pageToken: request.query.pageToken,
      })

      return response.send(result)
    }
  )

  fastify.get<loadTableRequest>(
    '/:prefix/namespaces/:namespace/tables/:table',
    {
      schema: loadTableSchema,
      exposeHeadRoute: false,
    },
    async (request, response) => {
      const bucket = await request.storage.findBucket(request.params.prefix, 'name,iceberg_catalog')

      if (!bucket.iceberg_catalog) {
        throw ERRORS.FeatureNotEnabled(request.params.prefix, 'iceberg_catalog')
      }

      const result = await request.icebergCatalog?.loadTable({
        warehouse: bucket.name,
        namespace: request.params.namespace,
        table: request.params.table,
      })

      return response.send(result)
    }
  )

  fastify.head<loadTableRequest>(
    '/:prefix/namespaces/:namespace/tables/:table',
    {
      schema: loadTableSchema,
    },
    async (request, response) => {
      const bucket = await request.storage.findBucket(request.params.prefix, 'name,iceberg_catalog')

      if (!bucket.iceberg_catalog) {
        throw ERRORS.FeatureNotEnabled(request.params.prefix, 'iceberg_catalog')
      }

      const result = await request.icebergCatalog?.tableExists({
        namespace: request.params.namespace,
        table: request.params.table,
      })

      return response.send(result)
    }
  )

  fastify.delete<dropTableSchemaRequest>(
    '/:prefix/namespaces/:namespace/tables/:table',
    {
      schema: dropTableSchema,
    },
    async (request, response) => {
      const bucket = await request.storage.findBucket(request.params.prefix, 'name,iceberg_catalog')

      if (!bucket.iceberg_catalog) {
        throw ERRORS.FeatureNotEnabled(request.params.prefix, 'iceberg_catalog')
      }

      const result = await request.icebergCatalog?.dropTable({
        namespace: request.params.namespace,
        table: request.params.table,
        warehouse: bucket.name,
      })

      return response.send(result)
    }
  )

  fastify.post<commitTableRequest>(
    '/:prefix/namespaces/:namespace/tables/:table',
    {
      schema: commitTransactionSchema,
    },
    async (request, response) => {
      const bucket = await request.storage.findBucket(request.params.prefix, 'name,iceberg_catalog')

      if (!bucket.iceberg_catalog) {
        throw ERRORS.FeatureNotEnabled(request.params.prefix, 'iceberg_catalog')
      }

      const result = await request.icebergCatalog?.updateTable({
        ...request.body,
        namespace: request.params.namespace,
        table: request.params.table,
      })

      return response.send(result)
    }
  )
}
