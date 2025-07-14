import { FastifyInstance } from 'fastify'
import { AuthenticatedRequest } from '../../types'
import { FromSchema } from 'json-schema-to-ts'
import { ERRORS } from '@internal/errors'
import { CreateTableRequest } from '@storage/protocols/iceberg/catalog/rest-catalog-client'
import { ROUTE_OPERATIONS } from '../operations'

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
  querystring: {
    type: 'object',
    properties: {
      purgeRequested: {
        type: 'string',
        enum: ['true', 'false', 'True', 'False'],
        default: 'false',
        description: 'If true, the table will be permanently deleted',
      },
    },
  },
  params: {
    type: 'object',
    properties: {
      prefix: { type: 'string', examples: ['prefix'] },
      namespace: { type: 'string', examples: ['namespace'] },
      table: { type: 'string', examples: ['table'] },
    },
    required: ['prefix', 'namespace', 'table'],
  },
  summary: 'Drop a Table',
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
  Querystring: FromSchema<(typeof dropTableSchema)['querystring']>
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
      schema: { ...createTableSchema, tags: ['iceberg'] },
    },
    async (request, response) => {
      if (!request.icebergCatalog) {
        throw ERRORS.FeatureNotEnabled('icebergCatalog', 'iceberg_catalog')
      }

      const result = await request.icebergCatalog.createTable({
        ...(request.body as unknown as CreateTableRequest),
        warehouse: request.params.prefix,
        namespace: request.params.namespace,
      })

      return response.send(result)
    }
  )

  fastify.get<listTableSchemaRequest>(
    '/:prefix/namespaces/:namespace/tables',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.ICEBERG_LIST_TABLES },
      },
      schema: { ...listTableSchema, tags: ['iceberg'] },
    },
    async (request, response) => {
      if (!request.icebergCatalog) {
        throw ERRORS.FeatureNotEnabled('icebergCatalog', 'iceberg_catalog')
      }

      const result = await request.icebergCatalog.listTables({
        warehouse: request.params.prefix,
        namespace: request.params.namespace,
        pageSize: request.query.pageSize,
        pageToken: request.query.pageToken,
      })

      return response.send(result)
    }
  )

  fastify.get<loadTableRequest>(
    '/:prefix/namespaces/:namespace/tables/:table',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.ICEBERG_LOAD_TABLE },
      },
      schema: { ...loadTableSchema, tags: ['iceberg'] },
      exposeHeadRoute: false,
    },
    async (request, response) => {
      if (!request.icebergCatalog) {
        throw ERRORS.FeatureNotEnabled('icebergCatalog', 'iceberg_catalog')
      }

      const result = await request.icebergCatalog.loadTable({
        warehouse: request.params.prefix,
        namespace: request.params.namespace,
        table: request.params.table,
      })

      return response.send(result)
    }
  )

  fastify.head<loadTableRequest>(
    '/:prefix/namespaces/:namespace/tables/:table',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.ICEBERG_TABLE_EXISTS },
      },
      schema: { ...loadTableSchema, tags: ['iceberg'] },
    },
    async (request, response) => {
      if (!request.icebergCatalog) {
        throw ERRORS.FeatureNotEnabled('icebergCatalog', 'iceberg_catalog')
      }

      const result = await request.icebergCatalog.tableExists({
        warehouse: request.params.prefix,
        namespace: request.params.namespace,
        table: request.params.table,
      })

      return response.status(204).send(result)
    }
  )

  fastify.register(async (fastify) => {
    fastify.addContentTypeParser(
      'application/json',
      { bodyLimit: 0 },
      (_request, _payload, done) => {
        done(null, null)
      }
    )

    fastify.delete<dropTableSchemaRequest>(
      '/:prefix/namespaces/:namespace/tables/:table',
      {
        config: {
          operation: { type: ROUTE_OPERATIONS.ICEBERG_DROP_TABLE },
        },
        schema: { ...dropTableSchema, tags: ['iceberg'] },
      },
      async (request, response) => {
        if (!request.icebergCatalog) {
          throw ERRORS.FeatureNotEnabled('icebergCatalog', 'iceberg_catalog')
        }

        const result = await request.icebergCatalog.dropTable({
          namespace: request.params.namespace,
          table: request.params.table,
          warehouse: request.params.prefix,
          purgeRequested: request.query.purgeRequested?.toLowerCase() === 'true',
        })

        return response.status(204).send(result)
      }
    )
  })

  fastify.post<commitTableRequest>(
    '/:prefix/namespaces/:namespace/tables/:table',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.ICEBERG_COMMIT_TABLE },
      },
      schema: { ...commitTransactionSchema, tags: ['iceberg'] },
    },
    async (request, response) => {
      if (!request.icebergCatalog) {
        throw ERRORS.FeatureNotEnabled('icebergCatalog', 'iceberg_catalog')
      }

      const result = await request.icebergCatalog.updateTable({
        ...request.body,
        namespace: request.params.namespace,
        table: request.params.table,
        warehouse: request.params.prefix,
      })

      return response.send(result)
    }
  )
}
