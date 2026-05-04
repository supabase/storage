import { ERRORS } from '@internal/errors'
import Ajv from 'ajv'
import { FastifyInstance, FastifySchemaCompiler } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const queryVectorFilterDefsSchema = {
  $id: 'queryVectorFilterDefsSchema',
  $defs: {
    Primitive: {
      anyOf: [{ type: 'string' }, { type: 'number' }, { type: 'boolean' }],
    },
    FieldOperators: {
      type: 'object',
      minProperties: 1,
      propertyNames: { pattern: '^\\$' },
      properties: {
        $eq: { $ref: '#/$defs/Primitive' },
        $ne: { $ref: '#/$defs/Primitive' },
        $gt: { type: 'number' },
        $gte: { type: 'number' },
        $lt: { type: 'number' },
        $lte: { type: 'number' },
        $in: { type: 'array', minItems: 1, items: { $ref: '#/$defs/Primitive' } },
        $nin: { type: 'array', minItems: 1, items: { $ref: '#/$defs/Primitive' } },
        $exists: { type: 'boolean' },
      },
      additionalProperties: false,
    },
    LogicalFilter: {
      anyOf: [
        {
          type: 'object',
          properties: {
            $and: {
              type: 'array',
              minItems: 1,
              items: { $ref: '#/$defs/Filter' },
            },
          },
          required: ['$and'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: {
            $or: {
              type: 'array',
              minItems: 1,
              items: { $ref: '#/$defs/Filter' },
            },
          },
          required: ['$or'],
          additionalProperties: false,
        },
      ],
    },
    Filter: {
      anyOf: [
        { $ref: '#/$defs/LogicalFilter' },
        {
          type: 'object',
          additionalProperties: {
            anyOf: [{ $ref: '#/$defs/Primitive' }, { $ref: '#/$defs/FieldOperators' }],
          },
        },
      ],
    },
  },
} as const

const queryVectorBodySchema = {
  $id: 'queryVectorBodySchema',
  type: 'object',
  properties: {
    filter: { $ref: 'queryVectorFilterDefsSchema#/$defs/Filter' },
    indexArn: { type: 'string' },
    indexName: {
      type: 'string',
      minLength: 3,
      maxLength: 45,
      pattern: '^[a-z0-9](?:[a-z0-9.-]{1,61})?[a-z0-9]$',
    },
    queryVector: {
      type: 'object',
      properties: {
        float32: { type: 'array', items: { type: 'number' } },
      },
      required: ['float32'],
      additionalProperties: false,
    },
    returnDistance: { type: 'boolean' },
    returnMetadata: { type: 'boolean' },
    topK: { type: 'number' },
    vectorBucketName: { type: 'string' },
  },
  required: ['vectorBucketName', 'indexName', 'queryVector', 'topK'],
  additionalProperties: false,
} as const

interface queryVectorRequest extends AuthenticatedRequest {
  Body: FromSchema<typeof queryVectorBodySchema>
}

export default async function routes(fastify: FastifyInstance) {
  // Register on Fastify's schema registry so @fastify/swagger can resolve the
  // body $ref when emitting /documentation/json.
  fastify.addSchema(queryVectorFilterDefsSchema)
  fastify.addSchema(queryVectorBodySchema)

  // Separate Ajv with removeAdditional disabled so request bodies are not
  // stripped before reaching the handler.
  const ajvNoRemoval = new Ajv({
    allErrors: true,
    removeAdditional: false,
    coerceTypes: false,
  })
  ajvNoRemoval.addSchema(queryVectorFilterDefsSchema)
  ajvNoRemoval.addSchema(queryVectorBodySchema)

  const perRouteValidator: FastifySchemaCompiler<any> = ({ schema }) => {
    const validate = ajvNoRemoval.compile(schema as object)
    return (data) => {
      const ok = validate(data)
      if (ok) return { value: data }
      return { error: new Error(JSON.stringify(validate.errors)) }
    }
  }

  fastify.post<queryVectorRequest>(
    '/QueryVectors',
    {
      validatorCompiler: perRouteValidator,
      config: {
        operation: { type: ROUTE_OPERATIONS.QUERY_VECTORS },
      },
      schema: {
        body: { $ref: 'queryVectorBodySchema#' },
        tags: ['vector'],
        summary: 'Query vectors',
      },
    },
    async (request, response) => {
      if (!request.s3Vector) {
        throw ERRORS.FeatureNotEnabled('vectorStore', 'Vector service not configured')
      }

      const indexResult = await request.s3Vector.queryVectors({
        vectorBucketName: request.body.vectorBucketName,
        indexName: request.body.indexName,
        indexArn: request.body.indexArn,
        queryVector: request.body.queryVector,
        topK: request.body.topK,
        filter: request.body.filter,
        returnDistance: request.body.returnDistance,
        returnMetadata: request.body.returnMetadata,
      })

      return response.send(indexResult)
    }
  )
}
