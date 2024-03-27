import { FromSchema } from 'json-schema-to-ts'

export const multipartUploadSchema = {
  $id: 'multipartUploadSchema',
  type: 'object',
  properties: {
    id: { type: 'string' },
    bucket_id: { type: 'string' },
    key: { type: 'string' },
    in_progress_size: { type: 'number' },
    upload_signature: { type: 'string' },
    version: { type: 'string' },
    created_at: { type: 'string' },
  },
  required: [
    'id',
    'bucket_id',
    'key',
    'version',
    'created_at',
    'in_progress_size',
    'upload_signature',
  ],
  additionalProperties: false,
} as const

export type S3MultipartUpload = FromSchema<typeof multipartUploadSchema>

export const uploadPartSchema = {
  $id: 'uploadPartSchema',
  type: 'object',
  properties: {
    id: { type: 'string' },
    upload_id: { type: 'string' },
    bucket_id: { type: 'string' },
    key: { type: 'string' },
    part_number: { type: 'number' },
    version: { type: 'string' },
    created_at: { type: 'string' },
    etag: { type: 'string' },
  },
  required: ['upload_id', 'bucket_id', 'key', 'version', 'part_number'],
  additionalProperties: false,
} as const

export type S3PartUpload = FromSchema<typeof uploadPartSchema>
