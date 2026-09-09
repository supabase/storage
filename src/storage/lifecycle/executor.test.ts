import { randomUUID } from 'node:crypto'
import { logSchema } from '@internal/monitoring'
import {
  lifecycleArtifactOutcomes,
  lifecycleBytesDeleted,
  lifecycleDeleteMarkersDeleted,
  lifecycleObjectVersionsDeleted,
} from '@internal/monitoring/metrics'
import * as config from '../../config'
import { newContinuation } from '../../test/utils/lifecycle-continuation'
import type { Database } from '../database'
import type {
  LifecycleCandidateIdentity,
  LifecycleCommitAttemptInput,
  LifecycleContinuation,
  LifecycleShardState,
} from '../schemas'
import type { Storage } from '../storage'
import {
  commitLifecycleBatchResults,
  freezeLifecycleBatchVersion,
  stageLifecycleBatch,
} from './continuation'
import { NoncurrentLifecycleExecutor } from './executor'

vi.mock('@internal/monitoring/metrics', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@internal/monitoring/metrics')>()),
  lifecycleArtifactOutcomes: { add: vi.fn() },
  lifecycleBytesDeleted: { add: vi.fn() },
  lifecycleDeleteMarkersDeleted: { add: vi.fn() },
  lifecycleObjectVersionsDeleted: { add: vi.fn() },
}))

const now = new Date('2026-08-15T00:00:00.000Z')

function configuration(generation: string) {
  return {
    id: 'bucket',
    name: 'bucket',
    type: 'STANDARD' as const,
    versioning_status: 'ENABLED' as const,
    lifecycle_configuration_generation: generation,
    lifecycle_configuration: {
      rules: [
        {
          status: 'Enabled' as const,
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 1 },
        },
      ],
    },
    lifecycle_shard_epoch: 1,
    lifecycle_shard_count: 1,
  }
}

function candidate(): LifecycleCandidateIdentity & { archivedAt: string } {
  return {
    id: randomUUID(),
    name: 'folder/object.txt',
    version: randomUUID(),
    isDeleteMarker: false,
    archivedAt: '2026-07-02T00:00:00.000Z',
  }
}

function stagedContinuation(generation: string, rows: ReturnType<typeof candidate>[]) {
  return stageLifecycleBatch(
    newContinuation(generation, { snapshotAt: '2026-08-01T00:00:00.000Z' }),
    {
      rawRowsExamined: rows.length,
      pageEnd: { name: rows.at(-1)!.name, archivedAt: rows.at(-1)!.archivedAt },
      versions: rows.map(freezeLifecycleBatchVersion),
    }
  )
}

function state(continuation: LifecycleContinuation | null = null): LifecycleShardState {
  return {
    bucketId: 'bucket',
    scanKind: 'NONCURRENT',
    shardId: 0,
    shardEpoch: '1',
    shardCount: 1,
    nextRunAt: now.toISOString(),
    claimId: randomUUID(),
    claimUntil: new Date(now.getTime() + 120_000).toISOString(),
    continuation,
    failureCount: 0,
  }
}

function createHarness(input: {
  continuation?: LifecycleContinuation
  deleteOutcome?: 'DELETED' | 'FAILED' | 'UNKNOWN'
  deleteOutcomesByCall?: Array<Array<'DELETED' | 'FAILED' | 'UNKNOWN'>>
  failureCount?: number
  jobBudgetMs?: number
}) {
  const generation = input.continuation?.generation ?? randomUUID()
  let persisted = input.continuation ?? null
  const claimed = state(persisted)
  claimed.failureCount = input.failureCount ?? 0
  const pageCandidate = candidate()
  const calls: string[] = []
  const database = {
    tenantId: 'tenant',
    hasMigration: vi.fn().mockResolvedValue(true),
    asSuperUser: vi.fn(() => database),
    claimLifecycleShard: vi
      .fn()
      .mockImplementation(() => ({ ...claimed, continuation: persisted })),
    findLifecycleBucket: vi.fn().mockResolvedValue(configuration(generation)),
    evaluateNoncurrentLifecyclePage: vi.fn().mockResolvedValue({
      rawRowsExamined: 1,
      candidates: [pageCandidate],
      pageEnd: {
        name: pageCandidate.name,
        archivedAt: pageCandidate.archivedAt,
      },
      exhausted: true,
    }),
    findLifecycleObjectVersions: vi
      .fn()
      .mockImplementation(
        (
          _bucketId: string,
          versions: Array<{ id: string; name: string; version: string | null }>
        ) => versions.map(({ id, name, version }) => ({ id, name, version }))
      ),
    armLifecycleAttempt: vi.fn().mockImplementation((attempt) => {
      persisted = attempt.continuation
      calls.push('arm')
      return { ...claimed, continuation: persisted }
    }),
    revalidateLifecycleShardClaim: vi.fn().mockImplementation(() => ({
      ...claimed,
      continuation: persisted,
    })),
    commitLifecycleAttempt: vi.fn().mockImplementation((attempt: LifecycleCommitAttemptInput) => {
      const versions = attempt.continuation.batch!.versions
      const successful = versions.filter((version) =>
        version.artifacts.every(
          (artifact) => artifact.outcome === 'DELETED' || artifact.outcome === 'ABSENT'
        )
      )
      const objectVersions = successful.filter((version) => version.artifacts.length > 0).length
      persisted = commitLifecycleBatchResults(attempt.continuation, {
        objectVersions,
        deleteMarkers: successful.length - objectVersions,
        bytes: objectVersions * 12,
      })
      calls.push('commit')
      return { ...claimed, continuation: persisted }
    }),
    revalidateLifecycleRecoveryAttempt: vi.fn().mockImplementation(() => ({
      ...claimed,
      continuation: persisted,
    })),
    releaseLifecycleShardClaim: vi.fn().mockImplementation((input) => {
      if (input.continuation !== undefined) persisted = input.continuation
      if (input.error) claimed.failureCount++
      return true
    }),
    completeLifecycleShardRun: vi.fn().mockImplementation(() => {
      calls.push('complete')
      return true
    }),
  }
  let deleteCall = 0
  const backend = {
    deleteObjectsDetailed: vi.fn().mockImplementation((_bucket: string, keys: string[]) => {
      calls.push('delete')
      const configuredOutcomes = input.deleteOutcomesByCall?.[deleteCall++]
      return keys.map((key, index) => {
        const outcome = configuredOutcomes?.[index] ?? input.deleteOutcome ?? 'DELETED'
        return {
          key,
          outcome,
          ...(outcome === 'DELETED' ? {} : { error: { message: outcome.toLowerCase() } }),
        }
      })
    }),
    headObject: vi.fn(),
  }
  const storage = {
    backend,
    db: database as unknown as Database,
    location: {
      getRootLocation: () => 'root',
      getKeyLocation: ({ tenantId, bucketId, objectName }: Record<string, string>) =>
        `${tenantId}/${bucketId}/${objectName}`,
    },
  } as unknown as Storage
  const executor = new NoncurrentLifecycleExecutor(storage, {
    now: () => now,
    pageSize: 500,
    jobBudgetMs: input.jobBudgetMs ?? 20_000,
    claimLeaseMs: 120_000,
    recoveryGraceMs: 1,
    fairnessDelayMs: 1000,
  })
  return {
    backend,
    calls,
    database,
    executor,
    generation,
    pageCandidate,
    storage,
    getPersisted: () => persisted,
  }
}

const coordinate = {
  bucketId: 'bucket',
  scanKind: 'NONCURRENT' as const,
  shardEpoch: '1',
  shardId: 0,
}

test.each([
  false,
  true,
])('preserves a claim decoding failure when release fails=%s', async (releaseFails) => {
  const harness = createHarness({})
  const failure = new Error('Unsupported lifecycle continuation version')
  const releaseFailure = new Error('database offline')
  harness.database.claimLifecycleShard.mockRejectedValueOnce(failure)
  if (releaseFails)
    harness.database.releaseLifecycleShardClaim.mockRejectedValueOnce(releaseFailure)
  const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})

  await expect(harness.executor.run(coordinate, { tenantLifecycleEnabled: true })).rejects.toBe(
    failure
  )
  expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledWith(
    expect.objectContaining({
      ...coordinate,
      claimId: expect.any(String),
      nextRunAt: expect.any(String),
      error: { name: 'Error', message: failure.message },
    })
  )
  expect(harness.database.releaseLifecycleShardClaim.mock.calls[0][0]).not.toHaveProperty(
    'continuation'
  )
  if (releaseFails) {
    expect(warning).toHaveBeenCalledWith(
      expect.anything(),
      expect.stringContaining('release lifecycle claim'),
      expect.objectContaining({ error: releaseFailure, tenantId: 'tenant' })
    )
  } else {
    expect(warning).not.toHaveBeenCalled()
  }
})

test('recovers a large journal with one bounded deletion request', async () => {
  const generation = randomUUID()
  const candidates = Array.from({ length: 500 }, (_, index) => ({
    ...candidate(),
    name: `object-${index}`,
  }))
  const initial = stagedContinuation(generation, candidates)
  initial.batch!.inFlight = {
    attemptId: randomUUID(),
    startedAt: '2026-08-01T00:00:00.000Z',
  }
  const harness = createHarness({ continuation: initial })
  harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValue({
    rawRowsExamined: 0,
    candidates: [],
    exhausted: true,
  })

  await expect(
    harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
  ).resolves.toMatchObject({ status: 'COMPLETED' })
  expect(harness.database.revalidateLifecycleRecoveryAttempt).toHaveBeenCalledOnce()
  expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledOnce()
  expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledWith(
    'root',
    initial.batch!.versions.flatMap((version) =>
      version.artifacts.map((artifact) => `tenant/bucket/${artifact.key}`)
    ),
    { signal: expect.any(AbortSignal) }
  )
  expect(harness.database.commitLifecycleAttempt).toHaveBeenCalledOnce()
  expect(
    harness.database.revalidateLifecycleRecoveryAttempt.mock.invocationCallOrder[0]
  ).toBeLessThan(harness.backend.deleteObjectsDetailed.mock.invocationCallOrder[0])
  expect(harness.getPersisted()?.batch).toBeUndefined()
  expect(harness.getPersisted()?.counters.objectVersionsDeleted).toBe(500)
})

describe('NoncurrentLifecycleExecutor', () => {
  it.each([
    'DELETED',
    'FAILED',
    'UNKNOWN',
  ] as const)('exports observed %s artifacts and only committed deletion counts', async (outcome) => {
    const artifacts = vi.spyOn(lifecycleArtifactOutcomes, 'add')
    const versions = vi.spyOn(lifecycleObjectVersionsDeleted, 'add')
    const bytes = vi.spyOn(lifecycleBytesDeleted, 'add')
    const harness = createHarness({ deleteOutcome: outcome })
    await harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    expect(artifacts).toHaveBeenCalledExactlyOnceWith(2, {
      scan_kind: 'NONCURRENT',
      outcome: outcome === 'UNKNOWN' ? 'UNRESOLVED' : outcome,
    })
    expect(versions).toHaveBeenCalledExactlyOnceWith(outcome === 'DELETED' ? 1 : 0, {
      scan_kind: 'NONCURRENT',
    })
    expect(bytes).toHaveBeenCalledExactlyOnceWith(outcome === 'DELETED' ? 12 : 0, {
      scan_kind: 'NONCURRENT',
    })
  })

  it('does not export a deletion when its metadata commit is not acknowledged', async () => {
    const versions = vi.spyOn(lifecycleObjectVersionsDeleted, 'add')
    const bytes = vi.spyOn(lifecycleBytesDeleted, 'add')
    const harness = createHarness({})
    harness.database.commitLifecycleAttempt.mockRejectedValueOnce(new Error('lost acknowledgement'))
    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).rejects.toThrow('lost acknowledgement')
    expect(versions).not.toHaveBeenCalled()
    expect(bytes).not.toHaveBeenCalled()
  })

  it('exports only new deletion counts when recovering a journal with prior progress', async () => {
    const initial = stagedContinuation(randomUUID(), [
      candidate(),
      { ...candidate(), isDeleteMarker: true },
    ])
    initial.counters.objectVersionsDeleted = 5
    initial.counters.deleteMarkersDeleted = 3
    initial.counters.bytesDeleted = 60
    initial.batch!.inFlight = {
      attemptId: randomUUID(),
      startedAt: '2026-08-01T00:00:00.000Z',
    }
    const harness = createHarness({ continuation: initial })
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValue({
      rawRowsExamined: 0,
      candidates: [],
      exhausted: true,
    })

    await harness.executor.run(coordinate, { tenantLifecycleEnabled: true })

    expect(lifecycleObjectVersionsDeleted.add).toHaveBeenCalledExactlyOnceWith(1, {
      scan_kind: 'NONCURRENT',
    })
    expect(lifecycleDeleteMarkersDeleted.add).toHaveBeenCalledExactlyOnceWith(1, {
      scan_kind: 'NONCURRENT',
    })
    expect(lifecycleBytesDeleted.add).toHaveBeenCalledExactlyOnceWith(12, {
      scan_kind: 'NONCURRENT',
    })
  })

  test('does not claim or recover work before the promotion fence is installed', async () => {
    const harness = createHarness({})
    harness.database.hasMigration.mockResolvedValue(false)
    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toEqual({ status: 'PAUSED' })
    expect(harness.database.claimLifecycleShard).not.toHaveBeenCalled()
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })
  it.each([
    'fresh',
    'staged',
  ] as const)('pauses a %s run under the closed tenant gate without changing its journal', async (progress) => {
    const continuation =
      progress === 'staged' ? stagedContinuation(randomUUID(), [candidate()]) : undefined
    const harness = createHarness({ continuation })

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: false })
    ).resolves.toEqual({
      status: 'PAUSED',
      runId: continuation?.runId,
    })

    expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        continuation: continuation ?? null,
        nextRunAt: new Date(now.getTime() + 1000).toISOString(),
      })
    )
    expect(harness.database.findLifecycleBucket).not.toHaveBeenCalled()
    expect(harness.database.evaluateNoncurrentLifecyclePage).not.toHaveBeenCalled()
    expect(harness.database.armLifecycleAttempt).not.toHaveBeenCalled()
    expect(harness.database.commitLifecycleAttempt).not.toHaveBeenCalled()
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  describe('with the tenant gate closed and a journal in flight', () => {
    function inFlightHarness(startedAt: string) {
      const generation = randomUUID()
      const initial = stagedContinuation(generation, [candidate()])
      initial.batch!.inFlight = {
        attemptId: randomUUID(),
        startedAt,
      }
      return createHarness({ continuation: initial })
    }

    it('recovers the journal, then pauses without arming a new batch', async () => {
      const harness = inFlightHarness('2026-08-01T00:00:00.000Z')

      await expect(
        harness.executor.run(coordinate, { tenantLifecycleEnabled: false })
      ).resolves.toMatchObject({ status: 'PAUSED' })
      expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledOnce()
      expect(harness.database.commitLifecycleAttempt).toHaveBeenCalledOnce()
      expect(harness.database.armLifecycleAttempt).not.toHaveBeenCalled()
      expect(harness.database.evaluateNoncurrentLifecyclePage).not.toHaveBeenCalled()
      expect(harness.database.findLifecycleBucket).not.toHaveBeenCalled()
      const released = harness.database.releaseLifecycleShardClaim.mock.calls.at(-1)![0]
      expect(released.continuation).toBe(harness.getPersisted())
      expect(released.continuation.batch).toBeUndefined()
      expect(new Date(released.nextRunAt).getTime() - now.getTime()).toBe(1000)
    })

    it('waits out the recovery grace window and pauses at its end', async () => {
      const harness = inFlightHarness(now.toISOString())

      await expect(
        harness.executor.run(coordinate, { tenantLifecycleEnabled: false })
      ).resolves.toMatchObject({ status: 'PAUSED' })
      expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
      expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledWith(
        expect.objectContaining({ nextRunAt: new Date(now.getTime() + 1).toISOString() })
      )
    })

    it('reports a partial recovery without releasing it as gated', async () => {
      const harness = inFlightHarness('2026-08-01T00:00:00.000Z')
      harness.backend.deleteObjectsDetailed.mockImplementationOnce((_bucket, keys: string[]) =>
        keys.map((key) => ({ key, outcome: 'UNKNOWN', error: { message: 'unknown' } }))
      )

      await expect(
        harness.executor.run(coordinate, { tenantLifecycleEnabled: false })
      ).resolves.toMatchObject({ status: 'PARTIAL' })
      expect(harness.database.armLifecycleAttempt).not.toHaveBeenCalled()
    })
  })

  it.each([
    { label: 'after an unknown result', error: 'timeout' },
    { label: 'of a journal armed but never sent', error: undefined },
  ])('keeps artifacts UNRESOLVED when a recovery $label reports FAILED', async ({ error }) => {
    const generation = randomUUID()
    const initial = stagedContinuation(generation, [candidate()])
    initial.batch!.inFlight = {
      attemptId: randomUUID(),
      startedAt: '2026-08-01T00:00:00.000Z',
    }
    initial.batch!.versions[0].artifacts.forEach((artifact) => {
      if (error) artifact.error = error
    })
    const harness = createHarness({ continuation: initial, deleteOutcome: 'FAILED' })

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'PARTIAL' })

    const commitInput = harness.database.commitLifecycleAttempt.mock
      .calls[0][0] as LifecycleCommitAttemptInput
    expect(commitInput.continuation.batch!.versions[0].artifacts).toEqual([
      expect.objectContaining({ outcome: 'UNRESOLVED', error: 'failed' }),
      expect.objectContaining({ outcome: 'UNRESOLVED', error: 'failed' }),
    ])
    expect(harness.getPersisted()?.batch?.inFlight).toBeDefined()
    expect(harness.database.completeLifecycleShardRun).not.toHaveBeenCalled()
  })

  it('resumes an unarmed staged batch from its persisted journal', async () => {
    const generation = randomUUID()
    const stagedCandidate = candidate()
    const initial = stagedContinuation(generation, [stagedCandidate])
    const harness = createHarness({ continuation: initial })

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })

    expect(harness.database.findLifecycleObjectVersions).toHaveBeenCalledWith('bucket', [
      expect.objectContaining({ name: stagedCandidate.name, version: stagedCandidate.version }),
    ])
    expect(harness.calls.indexOf('arm')).toBeLessThan(harness.calls.indexOf('delete'))
    expect(harness.backend.deleteObjectsDetailed.mock.calls[0]?.[1]).toEqual([
      `tenant/bucket/${stagedCandidate.name}/${stagedCandidate.version}`,
      `tenant/bucket/${stagedCandidate.name}/${stagedCandidate.version}.info`,
    ])
  })

  it('drops a disappeared staged row and advances instead of wedging the shard', async () => {
    const generation = randomUUID()
    const stagedCandidate = candidate()
    const initial = stagedContinuation(generation, [stagedCandidate])
    const harness = createHarness({ continuation: initial })
    harness.database.findLifecycleObjectVersions.mockResolvedValue([])
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValue({
      rawRowsExamined: 0,
      candidates: [],
      exhausted: true,
    })
    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })

    expect(harness.database.armLifecycleAttempt).not.toHaveBeenCalled()
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    expect(harness.database.evaluateNoncurrentLifecyclePage).toHaveBeenCalledWith(
      expect.objectContaining({ cursor: initial.pageEnd })
    )
    expect(harness.database.completeLifecycleShardRun).toHaveBeenCalledWith(
      expect.any(Object),
      expect.objectContaining({ counters: expect.objectContaining({ batchesCompleted: 1 }) }),
      expect.any(String)
    )
  })

  it.each([
    'lost',
    'unchanged',
  ] as const)('handles an arm rejection with a %s claim', async (claim) => {
    const initial = stagedContinuation(randomUUID(), [candidate()])
    const harness = createHarness({ continuation: initial })
    harness.database.armLifecycleAttempt.mockResolvedValueOnce(undefined)
    if (claim === 'lost')
      harness.database.revalidateLifecycleShardClaim.mockResolvedValueOnce(undefined)

    const run = harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    if (claim === 'unchanged') {
      await expect(run).rejects.toThrow('Lifecycle arm fence rejected')
    } else {
      await expect(run).resolves.toMatchObject({
        status: 'LOST_CLAIM',
      })
    }
    expect(harness.database.armLifecycleAttempt).toHaveBeenCalledOnce()
    expect(harness.database.revalidateLifecycleShardClaim).toHaveBeenCalledOnce()
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    if (claim === 'lost') {
      expect(harness.database.releaseLifecycleShardClaim).not.toHaveBeenCalled()
    } else {
      expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledWith(
        expect.objectContaining({
          nextRunAt: expect.any(String),
          ...(claim === 'unchanged' && { error: expect.any(Object) }),
        })
      )
    }
  })

  it.each([
    { label: 'deleted', generation: null },
    { label: 'regenerated', generation: randomUUID() },
  ])('pauses instead of failing when the configuration is $label between stage and arm', async ({
    generation,
  }) => {
    const initial = stagedContinuation(randomUUID(), [candidate()])
    const harness = createHarness({ continuation: initial })
    const bucket = configuration(initial.generation)
    harness.database.findLifecycleBucket
      .mockResolvedValueOnce(bucket)
      .mockResolvedValueOnce({ ...bucket, lifecycle_configuration_generation: generation })
    harness.database.armLifecycleAttempt.mockResolvedValueOnce(undefined)

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'PAUSED' })
    expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledOnce()
    expect(harness.database.releaseLifecycleShardClaim.mock.calls[0][0]).not.toHaveProperty('error')
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  it.each([
    { label: 'only the sidecar at the data key limit', nameLength: 973 },
    { label: 'both keys', nameLength: 974 },
  ])('records $label over the 1024-byte backend key limit as absent without sending it', async ({
    nameLength,
  }) => {
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      storageBackendType: 's3',
    })
    const harness = createHarness({})
    harness.pageCandidate.name = 'a'.repeat(nameLength)
    const dataKey = `tenant/bucket/${harness.pageCandidate.name}/${harness.pageCandidate.version}`
    expect(dataKey.length).toBe(nameLength + 51)

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })

    const commitInput = harness.database.commitLifecycleAttempt.mock
      .calls[0][0] as LifecycleCommitAttemptInput
    const outcomes = commitInput.continuation.batch!.versions[0].artifacts.map(
      (artifact) => artifact.outcome
    )
    if (dataKey.length <= 1024) {
      expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledExactlyOnceWith(
        'root',
        [dataKey],
        { signal: expect.any(AbortSignal) }
      )
      expect(outcomes).toEqual(['DELETED', 'ABSENT'])
    } else {
      expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
      expect(outcomes).toEqual(['ABSENT', 'ABSENT'])
    }
    expect(harness.database.completeLifecycleShardRun).toHaveBeenCalledOnce()
  })

  it('sends file backend keys beyond the S3 limit instead of assuming they are absent', async () => {
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      storageBackendType: 'file',
    })
    const harness = createHarness({})
    harness.pageCandidate.name = `${'directory/'.repeat(100)}object`
    const dataKey = `tenant/bucket/${harness.pageCandidate.name}/${harness.pageCandidate.version}`
    expect(Buffer.byteLength(dataKey)).toBeGreaterThan(1024)

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledExactlyOnceWith(
      'root',
      [dataKey, `${dataKey}.info`],
      { signal: expect.any(AbortSignal) }
    )
  })

  it('releases idle without scanning when no rule compiles', async () => {
    const harness = createHarness({})
    const bucket = configuration(harness.generation)
    harness.database.findLifecycleBucket.mockResolvedValue({
      ...bucket,
      lifecycle_configuration: {
        rules: [{ ...bucket.lifecycle_configuration.rules[0], status: 'Disabled' as const }],
      },
    })

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'PAUSED' })
    expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ continuation: null, nextRunAt: null })
    )
    expect(harness.database.evaluateNoncurrentLifecyclePage).not.toHaveBeenCalled()
  })

  it('advances the cursor past a non-exhausted empty page and continues', async () => {
    const harness = createHarness({})
    const pageEnd = { name: 'folder/skipped.txt', archivedAt: '2026-07-02T00:00:00.000Z' }
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValueOnce({
      rawRowsExamined: 1,
      candidates: [],
      pageEnd,
      exhausted: false,
    })

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.database.evaluateNoncurrentLifecyclePage).toHaveBeenCalledTimes(2)
    expect(harness.database.evaluateNoncurrentLifecyclePage.mock.calls[1][0].cursor).toEqual(
      pageEnd
    )
  })

  it.each([
    'completing the run',
    'committing the attempt',
  ])('reports a lost claim when %s is rejected', async (step) => {
    const harness = createHarness({})
    if (step === 'completing the run') {
      harness.database.completeLifecycleShardRun.mockResolvedValueOnce(false)
    } else {
      harness.database.commitLifecycleAttempt.mockResolvedValueOnce(undefined)
    }

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'LOST_CLAIM' })
  })

  it('releases a run that exhausts its job budget after the fairness delay', async () => {
    const harness = createHarness({ jobBudgetMs: 0 })

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'PARTIAL' })
    expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ nextRunAt: new Date(now.getTime() + 1000).toISOString() })
    )
  })

  it('measures the backend key limit in UTF-8 bytes', async () => {
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      storageBackendType: 's3',
    })
    const harness = createHarness({})
    harness.pageCandidate.name = 'é'.repeat(500)
    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  it('caps the retry delay at 15 minutes while a journal stays in flight', async () => {
    const harness = createHarness({ deleteOutcome: 'UNKNOWN', failureCount: 6 })

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'PARTIAL' })
    expect(harness.database.releaseLifecycleShardClaim).toHaveBeenLastCalledWith(
      expect.objectContaining({
        nextRunAt: new Date(now.getTime() + 15 * 60_000).toISOString(),
      })
    )
  })

  it('passes the error retry cap to release without rereading the journal', async () => {
    const harness = createHarness({ failureCount: 6 })
    const failure = new Error('acknowledgement lost')
    harness.database.commitLifecycleAttempt.mockRejectedValueOnce(failure)

    await expect(harness.executor.run(coordinate, { tenantLifecycleEnabled: true })).rejects.toBe(
      failure
    )
    expect(harness.database.revalidateLifecycleShardClaim).not.toHaveBeenCalled()
    const release = harness.database.releaseLifecycleShardClaim.mock.calls.at(-1)![0]
    expect(release).toMatchObject({
      nextRunAt: new Date(now.getTime() + 320 * 60_000).toISOString(),
      maxInFlightRetryAt: new Date(now.getTime() + 15 * 60_000).toISOString(),
    })
    expect(release).not.toHaveProperty('continuation')
  })

  it('arms a delete marker with null metadata and no backend artifacts', async () => {
    const harness = createHarness({})
    harness.pageCandidate.isDeleteMarker = true

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.database.armLifecycleAttempt).toHaveBeenCalledOnce()
    expect(
      harness.database.armLifecycleAttempt.mock.calls[0][0].continuation.batch.versions
    ).toEqual([
      {
        id: harness.pageCandidate.id,
        name: harness.pageCandidate.name,
        version: harness.pageCandidate.version,
        artifacts: [],
      },
    ])
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  it.each([
    null,
    '856d94e5-5241-42ae-9c0b-d06332a7ae15',
  ])('deletes exactly the physical version %s for an unversioned data row', async (version) => {
    const harness = createHarness({})
    harness.pageCandidate.version = version
    const name = harness.pageCandidate.name
    const keys = version === null ? [name] : [`${name}/${version}`, `${name}/${version}.info`]

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.database.findLifecycleObjectVersions).toHaveBeenCalledWith(coordinate.bucketId, [
      expect.objectContaining({ name, version }),
    ])
    expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledWith(
      'root',
      keys.map((key) => `tenant/bucket/${key}`),
      { signal: expect.any(AbortSignal) }
    )
    expect(harness.calls.indexOf('arm')).toBeLessThan(harness.calls.indexOf('delete'))
  })

  it.each([
    [{ message: '', code: 'AccessDenied' }, 'AccessDenied'],
    [{ message: '' }, 'Backend delete failed'],
  ])('records a non-empty error for the empty backend error %j', async (error, expected) => {
    const harness = createHarness({})
    harness.backend.deleteObjectsDetailed.mockImplementationOnce((_bucket, keys: string[]) =>
      keys.map((key) => ({ key, outcome: 'UNKNOWN', error }))
    )

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'PARTIAL' })
    expect(harness.getPersisted()?.batch?.versions[0].artifacts).toEqual([
      expect.objectContaining({ outcome: 'UNRESOLVED', error: expected }),
      expect.objectContaining({ outcome: 'UNRESOLVED', error: expected }),
    ])
  })

  it.each([
    {
      label: 'unknown version',
      version: '856d94e5-5241-42ae-9c0b-d06332a7ae15',
      outcomes: [
        ['UNKNOWN', 'UNKNOWN'],
        ['DELETED', 'DELETED'],
      ],
      pending: [0, 1],
      error: 'unknown',
    },
    {
      label: 'unknown null version',
      version: null,
      outcomes: [['UNKNOWN'], ['DELETED']],
      pending: [0],
      error: 'unknown',
    },
  ] as const)('backs off and recovers a $label by retrying only pending artifacts', async ({
    version,
    outcomes,
    pending,
    error,
  }) => {
    const harness = createHarness({ deleteOutcomesByCall: outcomes.map((call) => [...call]) })
    harness.pageCandidate.version = version
    const name = harness.pageCandidate.name
    const keys =
      version === null
        ? [`tenant/bucket/${name}`]
        : [`tenant/bucket/${name}/${version}`, `tenant/bucket/${name}/${version}.info`]

    for (let attempt = 0; attempt < outcomes.length - 1; attempt++) {
      await expect(
        harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
      ).resolves.toMatchObject({ status: 'PARTIAL' })
      const persisted = harness.getPersisted()!
      expect(persisted.batch?.inFlight).toBeDefined()
      expect(persisted.counters.objectVersionsDeleted).toBe(0)
      expect(persisted.batch!.versions[0].artifacts.map((artifact) => artifact.outcome)).toEqual(
        outcomes[0].map((outcome) => (outcome === 'UNKNOWN' ? 'UNRESOLVED' : outcome))
      )
      expect(harness.database.releaseLifecycleShardClaim).toHaveBeenLastCalledWith(
        expect.objectContaining({
          continuation: persisted,
          nextRunAt: new Date(now.getTime() + 5 * 60_000 * 2 ** attempt).toISOString(),
          error: expect.objectContaining({ message: error, pendingArtifacts: pending.length }),
        })
      )
      expect(harness.backend.deleteObjectsDetailed.mock.calls[attempt][1]).toEqual(
        attempt === 0 ? keys : pending.map((index) => keys[index])
      )
      expect(harness.database.completeLifecycleShardRun).not.toHaveBeenCalled()
      persisted.batch!.inFlight!.startedAt = '2026-08-14T00:00:00.000Z'
    }
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValue({
      rawRowsExamined: 0,
      candidates: [],
      exhausted: true,
    })
    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.backend.deleteObjectsDetailed.mock.lastCall![1]).toEqual(
      pending.map((index) => keys[index])
    )
    expect(harness.backend.headObject).not.toHaveBeenCalled()
    expect(harness.database.revalidateLifecycleRecoveryAttempt).toHaveBeenCalledTimes(
      outcomes.length - 1
    )
    expect(harness.database.commitLifecycleAttempt).toHaveBeenCalledTimes(outcomes.length)
    expect(harness.getPersisted()).toMatchObject({
      counters: { objectVersionsDeleted: 1, batchesCompleted: 1 },
    })
    expect(harness.getPersisted()?.batch).toBeUndefined()
  })

  it.each([
    { waitForGrace: true, status: 'PAUSED' },
    { waitForGrace: false, status: 'LOST_CLAIM' },
  ])('does no recovery I/O when an open gate still yields $status', async ({
    waitForGrace,
    status,
  }) => {
    const harness = createHarness({ deleteOutcome: 'UNKNOWN' })
    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status: 'PARTIAL' })
    const continuation = harness.getPersisted()!
    if (!waitForGrace) {
      continuation.batch!.inFlight!.startedAt = '2026-08-14T00:00:00.000Z'
      harness.database.revalidateLifecycleRecoveryAttempt.mockResolvedValueOnce(undefined)
    }
    harness.backend.deleteObjectsDetailed.mockClear()
    harness.database.commitLifecycleAttempt.mockClear()

    await expect(
      harness.executor.run(coordinate, { tenantLifecycleEnabled: true })
    ).resolves.toMatchObject({ status })
    expect(harness.database.revalidateLifecycleRecoveryAttempt).toHaveBeenCalledTimes(
      waitForGrace ? 0 : 1
    )
    expect(harness.backend.headObject).not.toHaveBeenCalled()
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    expect(harness.database.commitLifecycleAttempt).not.toHaveBeenCalled()
    expect(harness.getPersisted()).toBe(continuation)
  })

  it('rethrows a commit failure and releases the claim without overwriting the persisted continuation', async () => {
    const harness = createHarness({})
    const commit = harness.database.commitLifecycleAttempt.getMockImplementation()!
    const failure = new Error('connection lost after COMMIT')
    harness.database.commitLifecycleAttempt.mockImplementationOnce(async (input) => {
      commit(input)
      throw failure
    })

    await expect(harness.executor.run(coordinate, { tenantLifecycleEnabled: true })).rejects.toBe(
      failure
    )

    expect(harness.getPersisted()).toMatchObject({
      counters: { objectVersionsDeleted: 1, batchesCompleted: 1 },
    })
    expect(harness.getPersisted()!.batch).toBeUndefined()
    expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledWith(
      expect.objectContaining({
        nextRunAt: expect.any(String),
        error: { message: failure.message, name: 'Error' },
      })
    )
  })
})
