import { FromSchema } from 'json-schema-to-ts'

export const diskSchema = {
  $id: 'diskScbe a',
  type: 'object',
  properties: {
    id: { type: 'string' },
    name: { type: 'string', minLength: 3, maxLength: 200 },
    mount_point: { type: 'string' },
    credentials: {
      type: 'object',
      properties: {
        type: { type: 'string', enum: ['s3'] },
        access_key: { type: 'string' },
        secret_key: { type: 'string' },
        region: { type: 'string' },
        endpoint: { type: 'string' },
        force_path_style: { type: 'boolean', default: false },
      },
      required: ['access_key', 'secret_key', 'region', 'endpoint', 'type'],
    },
  },
  required: ['id', 'name', 'credentials'],
} as const

export type Disk = FromSchema<typeof diskSchema>
