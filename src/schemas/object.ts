import { bucketSchema } from './bucket'

export const objectSchema = {
  $id: 'objectSchema',
  type: 'object',
  properties: {
    name: { type: 'string' },
    bucket_id: { type: 'string' },
    owner: { type: 'string' },
    id: { anyOf: [{ type: 'string' }, { type: 'null' }] },
    updated_at: { anyOf: [{ type: 'string' }, { type: 'null' }] },
    created_at: { anyOf: [{ type: 'string' }, { type: 'null' }] },
    last_accessed_at: { anyOf: [{ type: 'string' }, { type: 'null' }] },
    metadata: {
      anyOf: [{ type: 'object', additionalProperties: true }, { type: 'null' }],
    },
    buckets: bucketSchema,
  },
  required: ['name'],
  additionalProperties: false,
} as const
