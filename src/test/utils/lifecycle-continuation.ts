import { randomUUID } from 'node:crypto'
import {
  createLifecycleContinuation,
  freezeLifecycleBatchVersion,
} from '@storage/lifecycle/continuation'
import type { LifecycleCandidateIdentity, NoncurrentLifecycleCursor } from '@storage/schemas'

export function newContinuation(
  generation: string,
  overrides: Partial<Parameters<typeof createLifecycleContinuation>[0]> = {}
) {
  return createLifecycleContinuation({
    runId: randomUUID(),
    generation,
    snapshotAt: new Date().toISOString(),
    ...overrides,
  })
}

export function batchOf(
  versions: LifecycleCandidateIdentity[],
  pageEnd: NoncurrentLifecycleCursor
) {
  return {
    rawRowsExamined: versions.length,
    pageEnd,
    versions: versions.map((version) => freezeLifecycleBatchVersion(version)),
  }
}
