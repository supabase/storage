import { FromSchema } from 'json-schema-to-ts'

export const credentialSchema = {
  $id: 'credentialSchema',
  type: 'object',
  properties: {
    id: { type: 'string' },
    name: { type: 'string' },
    force_path_style: { type: 'boolean' },
    access_key: { type: 'string' },
    secret_key: { type: 'string' },
    role: { type: 'string' },
    endpoint: { type: 'string' },
    region: { type: 'string', min: 1 },
    created_at: { type: 'string' },
  },
  required: ['id', 'name'],
  additionalProperties: false,
} as const

export type Credential = FromSchema<typeof credentialSchema>
