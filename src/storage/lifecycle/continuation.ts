import { withOptionalVersion } from '../backend/adapter'
import {
  LIFECYCLE_CONTINUATION_VERSION,
  LIFECYCLE_MAX_PAGE_SIZE,
  type LifecycleArtifact,
  type LifecycleBatchVersion,
  type LifecycleCandidateIdentity,
  type LifecycleContinuation,
  type LifecycleContinuationBatch,
  type LifecycleEvaluationPage,
  type LifecycleInFlightAttempt,
  type LifecycleRunCounters,
  type NoncurrentLifecycleShardCursor,
} from '../schemas/lifecycle'

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

interface LifecycleDeletionCounts {
  objectVersions: number
  deleteMarkers: number
  bytes: number
}

export class LifecycleContinuationDecodeError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'LifecycleContinuationDecodeError'
  }
}

export function createLifecycleContinuation(input: {
  runId: string
  generation: string
  snapshotAt: string
}): LifecycleContinuation {
  return decodeLifecycleContinuation({
    continuationVersion: LIFECYCLE_CONTINUATION_VERSION,
    ...input,
    counters: {
      versionsExamined: 0,
      versionsEligible: 0,
      objectVersionsDeleted: 0,
      deleteMarkersDeleted: 0,
      bytesDeleted: 0,
      batchesCompleted: 0,
    },
  })
}

export function freezeLifecycleBatchVersion(
  candidate: LifecycleCandidateIdentity
): LifecycleBatchVersion {
  const physicalKey = withOptionalVersion(candidate.name, candidate.version ?? undefined)
  return {
    id: candidate.id,
    name: candidate.name,
    version: candidate.version,
    artifacts: candidate.isDeleteMarker
      ? []
      : [
          { key: physicalKey, outcome: 'UNRESOLVED' },
          ...(candidate.version
            ? [{ key: `${physicalKey}.info`, outcome: 'UNRESOLVED' as const }]
            : []),
        ],
  }
}

export function stageLifecycleBatch(
  continuation: LifecycleContinuation,
  input: {
    pageEnd: NoncurrentLifecycleShardCursor
    rawRowsExamined: number
    versions: LifecycleBatchVersion[]
  }
): LifecycleContinuation {
  if (continuation.batch !== undefined) {
    throw new Error('Lifecycle continuation already has a staged batch')
  }
  if (input.versions.length === 0) {
    throw new Error('Lifecycle batch cannot be empty')
  }
  if (
    !Number.isSafeInteger(input.rawRowsExamined) ||
    input.rawRowsExamined < input.versions.length ||
    input.rawRowsExamined > LIFECYCLE_MAX_PAGE_SIZE
  ) {
    throw new Error('Lifecycle batch does not fit its raw page')
  }

  return decodeLifecycleContinuation({
    ...continuation,
    pageEnd: input.pageEnd,
    counters: {
      ...continuation.counters,
      versionsExamined: continuation.counters.versionsExamined + input.rawRowsExamined,
      versionsEligible: continuation.counters.versionsEligible + input.versions.length,
    },
    batch: { versions: input.versions },
  })
}

export function advanceEmptyLifecyclePage(
  continuation: LifecycleContinuation,
  page: LifecycleEvaluationPage
): LifecycleContinuation {
  if (continuation.batch !== undefined) {
    throw new Error('Lifecycle continuation already has a staged batch')
  }
  if (page.candidates.length !== 0) {
    throw new Error('Lifecycle pages with eligible candidates must be staged before advancing')
  }
  if (page.rawRowsExamined > 0 && page.pageEnd === undefined) {
    throw new Error('A non-empty lifecycle page requires pageEnd')
  }

  return decodeLifecycleContinuation({
    ...continuation,
    ...(page.pageEnd === undefined ? {} : { cursor: page.pageEnd }),
    counters: {
      ...continuation.counters,
      versionsExamined: continuation.counters.versionsExamined + page.rawRowsExamined,
    },
  })
}

export function armLifecycleAttempt(
  continuation: LifecycleContinuation,
  attempt: LifecycleInFlightAttempt
): LifecycleContinuation {
  if (!continuation.batch || continuation.batch.versions.length === 0) {
    throw new Error('Lifecycle continuation has no batch to arm')
  }
  if (continuation.batch.inFlight) {
    throw new Error('Lifecycle batch already has an armed attempt')
  }

  return decodeLifecycleContinuation({
    ...continuation,
    batch: { ...continuation.batch, inFlight: attempt },
  })
}

export function recordLifecycleArtifactOutcomes(
  continuation: LifecycleContinuation,
  outcomes: ReadonlyMap<string, Pick<LifecycleArtifact, 'outcome' | 'error'>>
): LifecycleContinuation {
  if (!continuation.batch?.inFlight) {
    throw new Error('Lifecycle batch must be armed before recording artifact outcomes')
  }

  const knownKeys = new Set(
    continuation.batch.versions.flatMap((version) =>
      version.artifacts.map((artifact) => artifact.key)
    )
  )
  for (const key of outcomes.keys()) {
    if (!knownKeys.has(key)) throw new Error(`Lifecycle outcome references unknown artifact ${key}`)
  }

  const versions = continuation.batch.versions.map((version) => ({
    ...version,
    artifacts: version.artifacts.map((artifact) => {
      const outcome = outcomes.get(artifact.key)
      return outcome ? { key: artifact.key, ...outcome } : artifact
    }),
  }))

  return decodeLifecycleContinuation({
    ...continuation,
    batch: { versions, inFlight: continuation.batch.inFlight },
  })
}

export function filterStagedLifecycleBatch(
  continuation: LifecycleContinuation,
  retainedVersions: ReadonlyArray<Pick<LifecycleBatchVersion, 'id'>>
): LifecycleContinuation {
  if (!continuation.batch || continuation.batch.inFlight) {
    throw new Error('Lifecycle batch must be staged and unarmed before filtering')
  }

  const versions = selectRetainedVersions(continuation.batch.versions, retainedVersions)
  if (versions.length === continuation.batch.versions.length) return continuation
  if (versions.length === 0) return advanceLifecycleBatch(continuation)

  return decodeLifecycleContinuation({
    ...continuation,
    batch: { versions },
  })
}

export function commitLifecycleBatchResults(
  continuation: LifecycleContinuation,
  deleted: LifecycleDeletionCounts
): LifecycleContinuation {
  if (!continuation.batch?.inFlight) {
    throw new Error('Lifecycle batch must be armed before committing results')
  }

  const versions = continuation.batch.versions.filter(lifecycleVersionNeedsCompensation)
  if (versions.length === 0) return advanceLifecycleBatch(continuation, deleted)

  return decodeLifecycleContinuation({
    ...continuation,
    counters: addDeletionCounters(continuation.counters, deleted),
    batch: { versions, inFlight: continuation.batch.inFlight },
  })
}

function advanceLifecycleBatch(
  continuation: LifecycleContinuation,
  deleted: LifecycleDeletionCounts = { objectVersions: 0, deleteMarkers: 0, bytes: 0 }
): LifecycleContinuation {
  if (!continuation.batch || !continuation.pageEnd) {
    throw new Error('Lifecycle continuation has no batch to advance')
  }

  const { batch: _batch, pageEnd: _pageEnd, ...rest } = continuation
  return decodeLifecycleContinuation({
    ...rest,
    cursor: continuation.pageEnd,
    counters: {
      ...addDeletionCounters(continuation.counters, deleted),
      batchesCompleted: continuation.counters.batchesCompleted + 1,
    },
  })
}

function lifecycleVersionNeedsCompensation(version: LifecycleBatchVersion): boolean {
  const outcomes = new Set(version.artifacts.map((artifact) => artifact.outcome))
  return (
    outcomes.has('UNRESOLVED') ||
    (outcomes.has('FAILED') && (outcomes.has('DELETED') || outcomes.has('ABSENT')))
  )
}

function selectRetainedVersions(
  currentVersions: LifecycleBatchVersion[],
  retainedVersions: ReadonlyArray<Pick<LifecycleBatchVersion, 'id'>>
): LifecycleBatchVersion[] {
  const ids = new Set(retainedVersions.map((version) => version.id))
  if (ids.size !== retainedVersions.length) {
    throw new Error('Lifecycle retained version identities must be unique')
  }

  const versions = currentVersions.filter((version) => ids.has(version.id))
  if (versions.length !== retainedVersions.length) {
    throw new Error('Lifecycle retained versions must belong to the current batch')
  }
  return versions
}

function addDeletionCounters(
  counters: LifecycleRunCounters,
  deleted: LifecycleDeletionCounts
): LifecycleRunCounters {
  return {
    ...counters,
    objectVersionsDeleted: counters.objectVersionsDeleted + deleted.objectVersions,
    deleteMarkersDeleted: counters.deleteMarkersDeleted + deleted.deleteMarkers,
    bytesDeleted: Math.min(counters.bytesDeleted + deleted.bytes, Number.MAX_SAFE_INTEGER),
  }
}

export function decodeLifecycleContinuation(value: unknown): LifecycleContinuation {
  const continuation = record(value, 'continuation')
  const continuationVersion = continuation.continuationVersion
  if (continuationVersion !== LIFECYCLE_CONTINUATION_VERSION) {
    throw decodeError(`Unsupported lifecycle continuation version ${String(continuationVersion)}`)
  }

  const decoded: LifecycleContinuation = {
    continuationVersion: LIFECYCLE_CONTINUATION_VERSION,
    runId: uuid(continuation.runId, 'runId'),
    generation: uuid(continuation.generation, 'generation'),
    snapshotAt: timestamp(continuation.snapshotAt, 'snapshotAt'),
    counters: decodeCounters(continuation.counters),
  }

  if (continuation.cursor !== undefined)
    decoded.cursor = decodeCursor(continuation.cursor, 'cursor')
  if (continuation.pageEnd !== undefined) {
    decoded.pageEnd = decodeCursor(continuation.pageEnd, 'pageEnd')
  }
  if (continuation.batch !== undefined) decoded.batch = decodeBatch(continuation.batch)

  if (decoded.batch?.versions.length && decoded.pageEnd === undefined) {
    throw decodeError('A lifecycle continuation batch requires pageEnd')
  }

  return decoded
}

function decodeCursor(value: unknown, label: string): NoncurrentLifecycleShardCursor {
  const cursor = record(value, label)
  return {
    archivedAt: timestamp(cursor.archivedAt, `${label}.archivedAt`),
    name: string(cursor.name, `${label}.name`),
  }
}

function decodeCounters(value: unknown): LifecycleRunCounters {
  const counters = record(value, 'counters')
  return {
    versionsExamined: nonnegativeInteger(counters.versionsExamined, 'counters.versionsExamined'),
    versionsEligible: nonnegativeInteger(counters.versionsEligible, 'counters.versionsEligible'),
    objectVersionsDeleted: nonnegativeInteger(
      counters.objectVersionsDeleted,
      'counters.objectVersionsDeleted'
    ),
    deleteMarkersDeleted: nonnegativeInteger(
      counters.deleteMarkersDeleted,
      'counters.deleteMarkersDeleted'
    ),
    bytesDeleted: nonnegativeInteger(counters.bytesDeleted, 'counters.bytesDeleted'),
    batchesCompleted: nonnegativeInteger(counters.batchesCompleted, 'counters.batchesCompleted'),
  }
}

function decodeBatch(value: unknown): LifecycleContinuationBatch {
  const batch = record(value, 'batch')
  if (!Array.isArray(batch.versions) || batch.versions.length > LIFECYCLE_MAX_PAGE_SIZE) {
    throw decodeError(`batch.versions must contain at most ${LIFECYCLE_MAX_PAGE_SIZE} entries`)
  }

  const versions = batch.versions.map((entry, index): LifecycleBatchVersion => {
    const version = record(entry, `batch.versions[${index}]`)
    const artifactCount = version.version ? 2 : 1
    if (
      !Array.isArray(version.artifacts) ||
      (version.artifacts.length !== 0 && version.artifacts.length !== artifactCount)
    ) {
      throw decodeError(
        `batch.versions[${index}].artifacts must contain 0 or ${artifactCount} entries`
      )
    }
    return {
      id: uuid(version.id, `batch.versions[${index}].id`),
      name: string(version.name, `batch.versions[${index}].name`),
      version:
        version.version === null
          ? null
          : string(version.version, `batch.versions[${index}].version`),
      artifacts: version.artifacts.map((artifact, artifactIndex) =>
        decodeArtifact(artifact, `batch.versions[${index}].artifacts[${artifactIndex}]`)
      ),
    }
  })

  const decoded: LifecycleContinuationBatch = { versions }
  if (batch.inFlight !== undefined) decoded.inFlight = decodeAttempt(batch.inFlight)
  return decoded
}

function decodeArtifact(value: unknown, label: string): LifecycleArtifact {
  const artifact = record(value, label)
  const decoded: LifecycleArtifact = {
    key: string(artifact.key, `${label}.key`),
    outcome: oneOf(
      artifact.outcome,
      ['UNRESOLVED', 'DELETED', 'ABSENT', 'FAILED'] as const,
      `${label}.outcome`
    ),
  }
  if (artifact.error !== undefined) decoded.error = string(artifact.error, `${label}.error`)
  return decoded
}

function decodeAttempt(value: unknown): LifecycleInFlightAttempt {
  const attempt = record(value, 'batch.inFlight')
  return {
    attemptId: uuid(attempt.attemptId, 'batch.inFlight.attemptId'),
    startedAt: timestamp(attempt.startedAt, 'batch.inFlight.startedAt'),
  }
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw decodeError(`${label} must be an object`)
  }
  return value as Record<string, unknown>
}

function string(value: unknown, label: string): string {
  if (typeof value !== 'string' || value.length === 0)
    throw decodeError(`${label} must be a string`)
  return value
}

function uuid(value: unknown, label: string): string {
  const parsed = string(value, label)
  if (!UUID.test(parsed)) throw decodeError(`${label} must be a UUID`)
  return parsed
}

function timestamp(value: unknown, label: string): string {
  const parsed = string(value, label)
  if (Number.isNaN(Date.parse(parsed))) throw decodeError(`${label} must be an ISO timestamp`)
  return parsed
}

function nonnegativeInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw decodeError(`${label} must be an integer between 0 and ${Number.MAX_SAFE_INTEGER}`)
  }
  return value as number
}

function oneOf<T extends readonly string[]>(value: unknown, values: T, label: string): T[number] {
  if (typeof value !== 'string' || !values.includes(value)) {
    throw decodeError(`${label} must be one of ${values.join(', ')}`)
  }
  return value as T[number]
}

function decodeError(message: string) {
  return new LifecycleContinuationDecodeError(message)
}
