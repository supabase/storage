import { randomUUID } from 'node:crypto'
import {
  armLifecycleAttempt,
  createLifecycleContinuation,
  freezeLifecycleBatchVersion,
  stageLifecycleBatch,
} from '@storage/lifecycle/continuation'
import type { LifecycleCandidateIdentity, NoncurrentLifecycleShardCursor } from '@storage/schemas'

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

export function armedContinuation(
  generation: string,
  versions: LifecycleCandidateIdentity[],
  pageEnd: NoncurrentLifecycleShardCursor,
  options: { snapshotAt?: string; startedAt?: string } = {}
) {
  return armLifecycleAttempt(
    stageLifecycleBatch(
      newContinuation(generation, options.snapshotAt ? { snapshotAt: options.snapshotAt } : {}),
      {
        rawRowsExamined: versions.length,
        pageEnd,
        versions: versions.map((version) => freezeLifecycleBatchVersion(version)),
      }
    ),
    { attemptId: randomUUID(), startedAt: options.startedAt ?? new Date().toISOString() }
  )
}
