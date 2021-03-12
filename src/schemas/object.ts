import { bucketSchema } from './bucket'

export const objectSchema = {
  $id: 'objectSchema',
  type: 'object',
  properties: {
    name: { type: 'string' },
    bucketId: { type: 'string' },
    owner: { type: 'string' },
    id: { anyOf: [{ type: 'string' }, { type: 'null' }] },
    updatedAt: { anyOf: [{ type: 'string' }, { type: 'null' }] },
    createdAt: { anyOf: [{ type: 'string' }, { type: 'null' }] },
    lastAccessedAt: { anyOf: [{ type: 'string' }, { type: 'null' }] },
    metadata: {
      anyOf: [{ type: 'object', additionalProperties: true }, { type: 'null' }],
    },
    buckets: bucketSchema,
  },
  required: ['name'],
  additionalProperties: false,
} as const
