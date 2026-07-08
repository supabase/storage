import { FromSchema } from 'json-schema-to-ts'

export const objectSchema = {
  $id: 'objectSchema',
  type: 'object',
  properties: {
    name: { type: 'string' },
    bucket_id: { type: 'string' },
    owner: { type: 'string' },
    owner_id: { type: 'string' },
    version: { type: 'string' },
    id: { type: 'string', nullable: true },
    updated_at: { type: 'string', nullable: true },
    created_at: { type: 'string', nullable: true },
    last_accessed_at: { type: 'string', nullable: true },
    metadata: {
      type: 'object',
      additionalProperties: true,
      nullable: true,
    },
    user_metadata: {
      type: 'object',
      additionalProperties: true,
      nullable: true,
    },
    buckets: { $ref: 'bucketSchema#' },
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

export type Obj = FromSchema<typeof objectSchema>
