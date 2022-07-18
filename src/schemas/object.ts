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
  examples: [
    {
      name: 'folder/cat.png',
      bucket_id: 'avatars',
      owner: '317eadce-631a-4429-a0bb-f19a7a517b4a',
      id: 'eaa8bdb5-2e00-4767-b5a9-d2502efe2196',
      updated_at: '2021-04-06T16:30:35.394674+00:00',
      created_at: '2021-04-06T16:30:35.394674+00:00',
      last_accessed_at: '2021-04-06T16:30:35.394674+00:00',
      metadata: {
        size: 1234,
      },
    },
  ],
} as const
