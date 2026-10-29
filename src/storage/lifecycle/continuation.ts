import { FILE_VERSION_SEPARATOR, PATH_SEPARATOR, SEPARATOR } from '../backend/adapter'
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
  type NoncurrentLifecycleCursor,
} from '../schemas/lifecycle'

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

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
  return {
    id: candidate.id,
    name: candidate.name,
    version: candidate.version,
    artifacts: candidate.isDeleteMarker
      ? []
      : lifecycleArtifactKeys(candidate.name, candidate.version).map((key) => ({
          key,
          outcome: 'UNRESOLVED' as const,
        })),
  }
}

function lifecycleArtifactKeys(
  name: string,
  version: string | null,
  separator = SEPARATOR
): string[] {
  const key = version ? `${name}${separator}${version}` : name
  return version ? [key, `${key}.info`] : [key]
}

export function encodeLifecycleContinuation(continuation: LifecycleContinuation): unknown {
  if (!continuation.batch) return continuation
  return {
    ...continuation,
    batch: {
      ...continuation.batch,
      versions: continuation.batch.versions.map((version) => ({
        ...version,
        artifacts: version.artifacts.map(({ key: _key, ...artifact }) => artifact),
      })),
    },
  }
}

export function stageLifecycleBatch(
  continuation: LifecycleContinuation,
  input: {
    pageEnd: NoncurrentLifecycleCursor
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

  return decodeLifecycleContinuation({
    ...continuation,
    pageEnd: input.pageEnd,
    counters: {
      ...continuation.counters,
      versionsExamined: continuation.counters.versionsExamined + input.rawRowsExamined,
      versionsEligible: continuation.counters.versionsEligible + input.versions.length,
    },
    batch: { keySeparator: SEPARATOR, versions: input.versions },
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
  if (continuation.batch?.inFlight) {
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

  const versions = continuation.batch.versions.map((version) => ({
    ...version,
    artifacts: version.artifacts.map((artifact) => {
      const outcome = outcomes.get(artifact.key)
      return outcome ? { key: artifact.key, ...outcome } : artifact
    }),
  }))

  return decodeLifecycleContinuation({
    ...continuation,
    batch: { ...continuation.batch, versions },
  })
}

export function commitLifecycleBatchResults(
  continuation: LifecycleContinuation,
  abandonPending = false
): LifecycleContinuation {
  if (!continuation.batch?.inFlight) {
    throw new Error('Lifecycle batch must be armed before committing results')
  }

  const versions = abandonPending
    ? []
    : continuation.batch.versions.filter(lifecycleVersionNeedsCompensation)
  if (versions.length === 0) return advanceLifecycleBatch(continuation)

  return decodeLifecycleContinuation({
    ...continuation,
    batch: { ...continuation.batch, versions },
  })
}

export function skipLifecycleBatch(continuation: LifecycleContinuation): LifecycleContinuation {
  const { batch: _batch, pageEnd: _pageEnd, ...rest } = continuation
  return decodeLifecycleContinuation({ ...rest, cursor: continuation.pageEnd })
}

function advanceLifecycleBatch(continuation: LifecycleContinuation): LifecycleContinuation {
  const skipped = skipLifecycleBatch(continuation)
  return decodeLifecycleContinuation({
    ...skipped,
    counters: {
      ...skipped.counters,
      batchesCompleted: skipped.counters.batchesCompleted + 1,
    },
  })
}

export function pendingArtifacts(
  continuation: LifecycleContinuation
): Array<LifecycleArtifact & Pick<LifecycleBatchVersion, 'name' | 'version'>> {
  return continuation.batch!.versions.flatMap(({ name, version, artifacts }) =>
    artifacts
      .filter((artifact) => artifact.outcome === 'UNRESOLVED' || artifact.outcome === 'FAILED')
      .map((artifact) => ({ ...artifact, name, version }))
  )
}

function lifecycleVersionNeedsCompensation(version: LifecycleBatchVersion): boolean {
  return version.artifacts.some(({ outcome }) => outcome === 'UNRESOLVED' || outcome === 'FAILED')
}

export function recordLifecycleMetadataDeletions(
  continuation: LifecycleContinuation,
  deleted: LifecycleDeletionCounts
): LifecycleContinuation {
  return { ...continuation, counters: addDeletionCounters(continuation.counters, deleted) }
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

export function decodeCursor(value: unknown, label: string): NoncurrentLifecycleCursor {
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
  const keySeparator = oneOf(
    batch.keySeparator,
    [PATH_SEPARATOR, FILE_VERSION_SEPARATOR] as const,
    'batch.keySeparator'
  )
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
    const name = string(version.name, `batch.versions[${index}].name`)
    const versionId =
      version.version === null ? null : string(version.version, `batch.versions[${index}].version`)
    const keys = lifecycleArtifactKeys(name, versionId, keySeparator)
    return {
      id: uuid(version.id, `batch.versions[${index}].id`),
      name,
      version: versionId,
      artifacts: version.artifacts.map((artifact, artifactIndex) =>
        decodeArtifact(
          artifact,
          keys[artifactIndex],
          `batch.versions[${index}].artifacts[${artifactIndex}]`
        )
      ),
    }
  })

  const decoded: LifecycleContinuationBatch = { keySeparator, versions }
  if (batch.inFlight !== undefined) decoded.inFlight = decodeAttempt(batch.inFlight)
  return decoded
}

function decodeArtifact(value: unknown, key: string, label: string): LifecycleArtifact {
  const artifact = record(value, label)
  const decoded: LifecycleArtifact = {
    key,
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
