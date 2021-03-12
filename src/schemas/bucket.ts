export const bucketSchema = {
  $id: 'bucketSchema',
  type: 'object',
  properties: {
    id: { type: 'string' },
    name: { type: 'string' },
    owner: { type: 'string' },
    createdAt: { type: 'string' },
    updatedAt: { type: 'string' },
  },
  required: ['id', 'name'],
  additionalProperties: false,
} as const
