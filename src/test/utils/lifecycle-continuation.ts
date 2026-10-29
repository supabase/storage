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
    trigger: 'scheduled',
    mode: 'DELETE',
    topology: { scanKind: 'NONCURRENT', epoch: '1', shardId: 0, shardCount: 1 },
    ...overrides,
  })
}

export function stagedContinuation(
  generation: string,
  versions: LifecycleCandidateIdentity[],
  pageEnd: NoncurrentLifecycleShardCursor,
  options: { snapshotAt?: string } = {}
) {
  return stageLifecycleBatch(
    newContinuation(generation, options.snapshotAt ? { snapshotAt: options.snapshotAt } : {}),
    {
      rawRowsExamined: versions.length,
      pageEnd,
      versions: versions.map((version) => freezeLifecycleBatchVersion(version)),
    }
  )
}

export function armedContinuation(
  generation: string,
  versions: LifecycleCandidateIdentity[],
  pageEnd: NoncurrentLifecycleShardCursor,
  options: { snapshotAt?: string; startedAt?: string } = {}
) {
  return armLifecycleAttempt(stagedContinuation(generation, versions, pageEnd, options), {
    attemptId: randomUUID(),
    authorizedGeneration: generation,
    startedAt: options.startedAt ?? new Date().toISOString(),
  })
}
