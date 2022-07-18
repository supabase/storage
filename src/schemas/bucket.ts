export const bucketSchema = {
  $id: 'bucketSchema',
  type: 'object',
  properties: {
    id: { type: 'string' },
    name: { type: 'string' },
    owner: { type: 'string' },
    public: { type: 'boolean' },
    created_at: { type: 'string' },
    updated_at: { type: 'string' },
  },
  required: ['id', 'name'],
  additionalProperties: false,
  examples: [
    {
      id: 'bucket2',
      name: 'bucket2',
      owner: '4d56e902-f0a0-4662-8448-a4d9e643c142',
      created_at: '2021-02-17T04:43:32.770206+00:00',
      updated_at: '2021-02-17T04:43:32.770206+00:00',
    },
  ],
} as const
