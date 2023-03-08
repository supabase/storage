import { bucketSchema } from './bucket'
import { FromSchema } from 'json-schema-to-ts'

export const uploadSchema = {
  $id: 'uploadSchema',
  type: 'object',
  properties: {
    name: { type: 'string' },
    bucket_id: { type: 'string' },
    owner: { type: 'string' },
    version: { type: 'string' },
    upload_type: { type: 'string', enum: ['STANDARD', 'MULTIPART'] },
    id: { type: 'string' },
    updated_at: { anyOf: [{ type: 'string' }, { type: 'null' }] },
    created_at: { anyOf: [{ type: 'string' }, { type: 'null' }] },
    expires_at: { anyOf: [{ type: 'string' }, { type: 'null' }] },
    buckets: bucketSchema,
  },
  required: ['id', 'name', 'bucket_id'],
  additionalProperties: false,
} as const

export type Upload = FromSchema<typeof uploadSchema>
