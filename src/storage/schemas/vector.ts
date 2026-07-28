import { FromSchema } from 'json-schema-to-ts'

const vectorIndex = {
  type: 'object',
  properties: {
    name: { type: 'string' },
    data_type: { type: 'string' },
    dimension: { type: 'number' },
    distance_metric: { type: 'string' },
    status: { type: 'string' },
    metadata_configuration: {
      type: 'object',
      properties: {
        nonFilterableMetadataKeys: {
          type: 'array',
          items: { type: 'string' },
        },
      },
    },
    bucket_id: { type: 'string' },
  },
  required: ['name', 'dimension', 'distance_metric', 'bucket_id'],
  additionalProperties: false,
} as const

export type VectorIndex = FromSchema<typeof vectorIndex>

// OpenAPI-documented response schemas for the vector bucket CRUD endpoints
// (src/http/routes/vector/{create,get,list,delete}-bucket.ts). Registered on
// Fastify via app.addSchema so they appear as named components in the
// exported spec, matching bucketSchema/objectSchema.
export const vectorBucketSchema = {
  $id: 'vectorBucketSchema',
  type: 'object',
  properties: {
    vectorBucketName: { type: 'string' },
    creationTime: {
      type: ['integer', 'null'],
      description: 'Unix timestamp (seconds) of when the bucket was created, if known.',
    },
  },
  required: ['vectorBucketName'],
  additionalProperties: false,
  examples: [{ vectorBucketName: 'embeddings-prod', creationTime: 1735689600 }],
} as const

export const getVectorBucketResponseSchema = {
  $id: 'getVectorBucketResponse',
  type: 'object',
  properties: {
    vectorBucket: { $ref: 'vectorBucketSchema#' },
  },
  required: ['vectorBucket'],
  additionalProperties: false,
} as const

export const listVectorBucketsResponseSchema = {
  $id: 'listVectorBucketsResponse',
  type: 'object',
  properties: {
    vectorBuckets: { type: 'array', items: { $ref: 'vectorBucketSchema#' } },
    nextToken: { type: 'string' },
  },
  required: ['vectorBuckets'],
  additionalProperties: false,
} as const
