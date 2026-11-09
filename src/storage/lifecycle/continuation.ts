import { SEPARATOR } from '../backend/adapter'
import {
  LIFECYCLE_CONTINUATION_VERSION,
  type LifecycleBatch,
  type LifecycleBatchVersion,
  type LifecycleCandidateIdentity,
  type LifecycleContinuation,
  type LifecycleEvaluationPage,
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
      : lifecycleArtifactKeys(candidate.name, candidate.version),
  }
}

function lifecycleArtifactKeys(name: string, version: string | null): string[] {
  const key = version ? `${name}${SEPARATOR}${version}` : name
  return version ? [key, `${key}.info`] : [key]
}

export function advanceEmptyLifecyclePage(
  continuation: LifecycleContinuation,
  page: LifecycleEvaluationPage
): LifecycleContinuation {
  if (page.candidates.length !== 0) {
    throw new Error('Lifecycle pages with eligible candidates must be armed before advancing')
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

export function advanceLifecycleBatch(
  continuation: LifecycleContinuation,
  batch: LifecycleBatch,
  deleted: LifecycleDeletionCounts
): LifecycleContinuation {
  const counters = addDeletionCounters(continuation.counters, deleted)
  return decodeLifecycleContinuation({
    ...continuation,
    cursor: batch.pageEnd,
    counters: {
      ...counters,
      versionsExamined: counters.versionsExamined + batch.rawRowsExamined,
      versionsEligible: counters.versionsEligible + batch.versions.length,
      batchesCompleted:
        counters.batchesCompleted + (deleted.objectVersions + deleted.deleteMarkers > 0 ? 1 : 0),
    },
  })
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
  if (continuation.batch !== undefined) {
    throw decodeError('A lifecycle continuation cannot carry a batch')
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

function decodeError(message: string) {
  return new LifecycleContinuationDecodeError(message)
}
