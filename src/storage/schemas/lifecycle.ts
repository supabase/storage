import type { Bucket } from './bucket'

export const LIFECYCLE_MAX_RULES = 1000
export const LIFECYCLE_MAX_NONCURRENT_DAYS = 2147483647
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
    noncurrentVersionExpiration: {
      type: 'object',
      properties: {
        noncurrentDays: {
          type: 'integer',
          finite: true,
          description: `Must be between 1 and ${LIFECYCLE_MAX_NONCURRENT_DAYS}.`,
        },
        newerNoncurrentVersions: { type: 'integer', finite: true },
      },
      required: ['noncurrentDays'],
    },
  },
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
        required: ['status', 'filter', 'noncurrentVersionExpiration'],
      },
    },
  },
  required: ['rules'],
} as const

export interface NoncurrentVersionExpiration {
  noncurrentDays: number
  newerNoncurrentVersions?: number
}

export type LifecycleRuleFilter = Record<string, never>

// Persisted configuration follows the same contract as normalized writes.
export interface LifecycleRule {
  id?: string
  status: 'Enabled' | 'Disabled'
  noncurrentVersionExpiration: NoncurrentVersionExpiration
  filter: LifecycleRuleFilter
}

export interface BucketLifecycleConfiguration {
  rules: LifecycleRule[]
}

export type LifecycleBucket = Pick<Bucket, 'id' | 'name' | 'type'> & {
  lifecycle_configuration: BucketLifecycleConfiguration | null
  lifecycle_configuration_generation: string | null
}
