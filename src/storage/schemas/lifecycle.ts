import type { Bucket } from './bucket'

export const LIFECYCLE_MAX_RULES = 1000
export const LIFECYCLE_MAX_NEWER_NONCURRENT_VERSIONS = 100

// Keep additional fields visible to the semantic validator instead of allowing
// Fastify to strip them before we can return a useful unsupported-field error.
const lifecycleRuleSchema = {
  type: 'object',
  properties: {
    id: {
      type: 'string',
      description: 'Rule IDs are limited to 255 characters.',
    },
    status: { type: 'string', enum: ['Enabled', 'Disabled'] },
    filter: { type: 'object', additionalProperties: true },
    legacyPrefix: { type: 'string' },
    noncurrentVersionExpiration: {
      type: 'object',
      properties: {
        noncurrentDays: { type: 'integer', finite: true },
        newerNoncurrentVersions: { type: 'integer', finite: true },
      },
      required: ['noncurrentDays'],
    },
  },
  anyOf: [{ required: ['filter'] }, { required: ['legacyPrefix'] }],
} as const

export const bucketLifecycleConfigurationSchema = {
  type: 'object',
  properties: {
    rules: {
      type: 'array',
      minItems: 1,
      maxItems: LIFECYCLE_MAX_RULES,
      items: {
        ...lifecycleRuleSchema,
        required: ['status', 'noncurrentVersionExpiration'],
      },
    },
  },
  required: ['rules'],
} as const

export const bucketLifecycleConfigurationReadSchema = {
  ...bucketLifecycleConfigurationSchema,
  properties: {
    ...bucketLifecycleConfigurationSchema.properties,
    rules: {
      ...bucketLifecycleConfigurationSchema.properties.rules,
      items: {
        ...lifecycleRuleSchema,
        required: ['status'],
      },
    },
  },
} as const

export interface NoncurrentVersionExpiration {
  noncurrentDays: number
  newerNoncurrentVersions?: number
}

export type LifecycleRuleFilter = Record<string, never>

// Reads preserve stored rules without applying the current write validator.
export interface StoredLifecycleRule {
  id?: string
  status: 'Enabled' | 'Disabled'
  filter?: Record<string, unknown>
  legacyPrefix?: string
  noncurrentVersionExpiration?: NoncurrentVersionExpiration
}

// Normalized v1 writes require expiration and support only whole-bucket selectors.
export interface LifecycleRule extends StoredLifecycleRule {
  filter?: LifecycleRuleFilter
  legacyPrefix?: ''
  noncurrentVersionExpiration: NoncurrentVersionExpiration
}

export interface StoredBucketLifecycleConfiguration extends Record<string, unknown> {
  rules: StoredLifecycleRule[]
}

export interface BucketLifecycleConfiguration extends StoredBucketLifecycleConfiguration {
  rules: LifecycleRule[]
}

export type LifecycleBucket = Pick<Bucket, 'id' | 'name' | 'type'> & {
  lifecycle_configuration: StoredBucketLifecycleConfiguration | null
  lifecycle_configuration_generation: string | null
}

export interface LifecycleConfigurationMutationResult {
  bucket: LifecycleBucket
  changed: boolean
}
