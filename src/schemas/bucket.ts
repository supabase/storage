export const bucketSchema = {
  $id: 'bucketSchema',
  type: 'object',
  properties: {
    id: { type: 'string' },
    name: { type: 'string' },
    owner: { type: 'string' },
    created_at: { type: 'string' },
    updated_at: { type: 'string' },
  },
  required: ['id', 'name'],
  additionalProperties: false,
} as const
