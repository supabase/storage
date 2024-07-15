import { FromSchema } from 'json-schema-to-ts'
import { Disk } from './disk'

export const bucketSchema = {
  $id: 'bucketSchema',
  type: 'object',
  properties: {
    id: { type: 'string' },
    name: { type: 'string' },
    owner: { type: 'string' },
    public: { type: 'boolean' },
    file_size_limit: { type: ['integer', 'null'] },
    disk_id: { type: ['string', 'null'] },
    allowed_mime_types: { type: ['array', 'null'], items: { type: 'string' } },
    created_at: { type: 'string' },
    updated_at: { type: 'string' },
  },
  required: ['id', 'name'],
  additionalProperties: false,
  examples: [
    {
      id: 'bucket2',
      name: 'bucket2',
      public: false,
      file_size_limit: 1000000,
      allowed_mime_types: ['image/png', 'image/jpeg'],
      owner: '4d56e902-f0a0-4662-8448-a4d9e643c142',
      created_at: '2021-02-17T04:43:32.770206+00:00',
      updated_at: '2021-02-17T04:43:32.770206+00:00',
    },
  ],
} as const

export type Bucket = FromSchema<typeof bucketSchema>
export type BucketWithDisk = Bucket &
  Partial<Omit<Disk, 'id' | 'name'>> & {
    disk_id?: string
    mount_point: string
    credentials: {
      bucket: string
      access_key: string
      secret_key: string
      region: string
      endpoint: string
      force_path_style: boolean
    }
  }
