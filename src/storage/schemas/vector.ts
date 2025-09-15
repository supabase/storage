import { FromSchema } from 'json-schema-to-ts'

const vectorIndex = {
  type: 'object',
  properties: {
    id: { type: 'string' },
    dataType: { type: 'string' },
    dimension: { type: 'number' },
    distanceMetric: { type: 'string' },
    metadataConfiguration: {
      type: 'object',
      properties: {
        nonFilterableMetadataKeys: {
          type: 'array',
          items: { type: 'string' },
        },
      },
    },
    bucketId: { type: 'string' },
  },
  required: ['id', 'dimension', 'distanceMetric', 'bucketId'],
  additionalProperties: false,
} as const

export type VectorIndex = FromSchema<typeof vectorIndex>
