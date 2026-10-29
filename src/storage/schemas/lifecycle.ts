import type { Bucket, BucketVersioningStatus } from './bucket'

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

export interface LifecycleEvaluationRule {
  cutoffAt: string
  newerNoncurrentVersions?: number
}

export const LIFECYCLE_MAX_PAGE_SIZE = 480
// Stay below PostgreSQL 64 cached subxids
export const LIFECYCLE_MAX_DELETE_BATCH_SIZE = 48

export interface NoncurrentLifecycleCursor {
  archivedAt: string
  name: string
}

export interface LifecycleCandidateIdentity {
  id: string
  name: string
  version: string | null
  isDeleteMarker: boolean
}

export interface LifecycleEvaluatedCandidate extends LifecycleCandidateIdentity {
  archivedAt: string
  // One-based position in the raw page including ineligible rows.
  rawRowNumber: number
}

export interface LifecycleEvaluationPage {
  rawRowsExamined: number
  candidates: LifecycleEvaluatedCandidate[]
  pageEnd?: NoncurrentLifecycleCursor
  exhausted: boolean
}

export interface EvaluateNoncurrentLifecyclePageInput {
  bucketId: string
  snapshotAt: string
  cursor?: NoncurrentLifecycleCursor
  rules: LifecycleEvaluationRule[]
  pageSize: number
}

export const LIFECYCLE_CONTINUATION_VERSION = 1 as const

export interface LifecycleRunCounters {
  versionsExamined: number
  versionsEligible: number
  objectVersionsDeleted: number
  deleteMarkersDeleted: number
  bytesDeleted: number
  batchesCompleted: number
}

export type LifecycleArtifactOutcome = 'UNRESOLVED' | 'DELETED' | 'ABSENT' | 'FAILED'

export interface LifecycleArtifact {
  key: string
  outcome: LifecycleArtifactOutcome
  error?: string
}

export interface LifecycleBatchVersion {
  id: string
  name: string
  version: string | null
  artifacts: LifecycleArtifact[]
}

export interface LifecycleInFlightAttempt {
  attemptId: string
  startedAt: string
}

export interface LifecycleContinuationBatch {
  keySeparator: '/' | '-$v-'
  versions: LifecycleBatchVersion[]
  inFlight?: LifecycleInFlightAttempt
}

export interface LifecycleContinuation {
  continuationVersion: typeof LIFECYCLE_CONTINUATION_VERSION
  runId: string
  generation: string
  snapshotAt: string
  cursor?: NoncurrentLifecycleCursor
  pageEnd?: NoncurrentLifecycleCursor
  counters: LifecycleRunCounters
  batch?: LifecycleContinuationBatch
}

export interface LifecycleClaimIdentity {
  bucketId: string
  claimId: string
}

export interface LifecycleClaimInput extends LifecycleClaimIdentity {
  leaseMs: number
}

export interface LifecycleArmAttemptInput extends LifecycleClaimIdentity {
  leaseMs: number
  continuation: LifecycleContinuation
}

export interface LifecycleCommitAttemptInput extends LifecycleClaimIdentity {
  attemptId: string
  continuation: LifecycleContinuation
  abandonPending?: boolean
}

export interface LifecycleReleaseClaimInput extends LifecycleClaimIdentity {
  // Omit after an ambiguous write to preserve the durable journal.
  continuation?: LifecycleContinuation | null
  nextRunAt: string | null
  // Cap a retry only when preserving a persisted journal in flight.
  maxInFlightRetryAt?: string
  error?: Record<string, unknown>
}

export interface LifecycleDispatchState {
  nextDispatchAt: Date | null
  maxFailureCount: number
  oldestInFlightStartedAt: string | null
  hasAbandonedArtifacts: boolean
}

export interface LifecycleState {
  bucketId: string
  claimId: string | null
  continuation: LifecycleContinuation | null
  failureCount: number
}

export type LifecycleClaimedState = LifecycleState & {
  bucket: Pick<
    LifecycleBucket,
    'versioning_status' | 'lifecycle_configuration' | 'lifecycle_configuration_generation'
  >
}

export type LifecycleBucket = Pick<Bucket, 'id' | 'name' | 'type'> & {
  type: 'STANDARD' | 'ANALYTICS'
  versioning_status: BucketVersioningStatus
  lifecycle_configuration: BucketLifecycleConfiguration | null
  lifecycle_configuration_generation: string | null
}
