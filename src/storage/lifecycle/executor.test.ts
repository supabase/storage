import { randomUUID } from 'node:crypto'
import { logSchema } from '@internal/monitoring'
import type { Database } from '../database'
import type {
  LifecycleCandidate,
  LifecycleCommitAttemptInput,
  LifecycleContinuation,
  LifecycleShardState,
} from '../schemas'
import type { Storage } from '../storage'
import {
  commitLifecycleBatchResults,
  createLifecycleContinuation,
  freezeLifecycleBatchVersion,
  lifecycleVersionNeedsCompensation,
  stageLifecycleBatch,
} from './continuation'
import { NoncurrentLifecycleExecutor } from './executor'

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

function candidate(generation: string): LifecycleCandidate {
  return {
    id: randomUUID(),
    bucketId: 'bucket',
    name: 'folder/object.txt',
    version: randomUUID(),
    isVersioned: true,
    isDeleteMarker: false,
    metadata: {
      size: 12,
      generation,
    },
    createdAt: '2026-07-01T00:00:01.000Z',
    archivedAt: '2026-07-02T00:00:00.000Z',
  }
}

function state(
  generation: string,
  continuation: LifecycleContinuation | null = null
): LifecycleShardState {
  return {
    bucketId: 'bucket',
    scanKind: 'NONCURRENT',
    shardId: 0,
    shardEpoch: '1',
    shardCount: 1,
    configurationGeneration: generation,
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
  headMissing?: boolean
  deleteEnabled?: boolean
}) {
  const generation = input.continuation?.generation ?? randomUUID()
  let persisted = input.continuation ?? null
  const claimed = state(generation, persisted)
  const pageCandidate = candidate(generation)
  const calls: string[] = []
  const database = {
    tenantId: 'tenant',
    asSuperUser: vi.fn(() => database),
    claimLifecycleShard: vi
      .fn()
      .mockImplementation(() => ({ ...claimed, continuation: persisted })),
    findLifecycleBucket: vi.fn().mockResolvedValue(configuration(generation)),
    saveLifecycleContinuation: vi.fn().mockImplementation((_identity, continuation) => {
      persisted = continuation
      calls.push(continuation.batch ? 'stage' : 'save')
      return true
    }),
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
        (_bucketId: string, versions: Array<{ name: string; version: string | null }>) =>
          versions.map((version) => ({
            id: pageCandidate.id,
            bucket_id: pageCandidate.bucketId,
            name: version.name,
            version: version.version,
            is_versioned: pageCandidate.isVersioned,
            is_delete_marker: pageCandidate.isDeleteMarker,
            metadata: pageCandidate.metadata,
            created_at: new Date(pageCandidate.createdAt),
            archived_at: new Date(pageCandidate.archivedAt),
          }))
      ),
    armLifecycleAttempt: vi.fn().mockImplementation((attempt) => {
      persisted = attempt.continuation
      calls.push('arm')
      return { ...claimed, continuation: persisted }
    }),
    revalidateLifecycleShardClaim: vi.fn().mockImplementation(() => ({
      ...claimed,
      continuation: persisted,
      configurationGeneration: generation,
    })),
    commitLifecycleAttempt: vi.fn().mockImplementation((attempt: LifecycleCommitAttemptInput) => {
      const versions = attempt.continuation.batch!.versions
      const successful = versions.filter((version) =>
        version.artifacts.every(
          (artifact) => artifact.outcome === 'DELETED' || artifact.outcome === 'ABSENT'
        )
      )
      const retained = versions.filter(lifecycleVersionNeedsCompensation)
      persisted = commitLifecycleBatchResults(attempt.continuation, retained, {
        objectVersions: successful.length,
        deleteMarkers: 0,
        bytes: successful.length * 12,
      })
      calls.push('commit')
      return { ...claimed, continuation: persisted }
    }),
    revalidateLifecycleRecoveryAttempt: vi.fn().mockImplementation(() => ({
      ...claimed,
      continuation: persisted,
    })),
    releaseLifecycleShardClaim: vi.fn().mockImplementation((input) => {
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
    headObject: input.headMissing
      ? vi.fn().mockRejectedValue(Object.assign(new Error('missing'), { code: 'ENOENT' }))
      : vi.fn().mockResolvedValue({ size: 12 }),
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
    deleteEnabled: input.deleteEnabled ?? true,
    pageSize: 500,
    jobBudgetMs: 20_000,
    claimLeaseMs: 120_000,
    recoveryGraceMs: 1,
    fairnessDelayMs: 1000,
    evaluationIntervalMs: 86_400_000,
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

test('releases a claim whose persisted continuation cannot be decoded', async () => {
  const harness = createHarness({})
  const failure = new Error('Unsupported lifecycle continuation version')
  harness.database.claimLifecycleShard.mockRejectedValueOnce(failure)

  await expect(harness.executor.run(coordinate, { tenantVersioningEnabled: true })).rejects.toBe(
    failure
  )
  expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledWith(
    expect.objectContaining({
      ...coordinate,
      claimId: expect.any(String),
      nextRunAt: expect.any(String),
      error: expect.any(Object),
    })
  )
  expect(harness.database.releaseLifecycleShardClaim.mock.calls[0][0]).not.toHaveProperty(
    'continuation'
  )
})

test('preserves the original failure when releasing its claim also fails', async () => {
  const harness = createHarness({})
  const failure = new Error('invalid continuation')
  const releaseFailure = new Error('database offline')
  harness.database.claimLifecycleShard.mockRejectedValueOnce(failure)
  harness.database.releaseLifecycleShardClaim.mockRejectedValueOnce(releaseFailure)
  const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
  try {
    await expect(harness.executor.run(coordinate, { tenantVersioningEnabled: true })).rejects.toBe(
      failure
    )
    expect(warning).toHaveBeenCalledWith(
      expect.anything(),
      expect.stringContaining('release lifecycle claim'),
      expect.objectContaining({ error: releaseFailure, tenantId: 'tenant' })
    )
  } finally {
    warning.mockRestore()
  }
})

test.each([
  true,
  false,
])('recovers a large journal with deletion enabled=%s', async (deleteEnabled) => {
  const generation = randomUUID()
  const candidates = Array.from({ length: 500 }, (_, index) => ({
    ...candidate(generation),
    name: `object-${index}`,
  }))
  const initial = stageLifecycleBatch(
    createLifecycleContinuation({
      runId: randomUUID(),
      trigger: 'recovery',
      generation,
      snapshotAt: '2026-08-01T00:00:00.000Z',
      topology: { scanKind: 'NONCURRENT', epoch: '1', shardId: 0, shardCount: 1 },
    }),
    {
      rawRowsExamined: candidates.length,
      pageEnd: { name: candidates.at(-1)!.name, archivedAt: candidates.at(-1)!.archivedAt },
      versions: candidates.map(freezeLifecycleBatchVersion),
    }
  )
  initial.batch!.inFlight = {
    attemptId: randomUUID(),
    authorizedGeneration: generation,
    startedAt: '2026-08-01T00:00:00.000Z',
  }
  const harness = createHarness({ continuation: initial })
  let elapsed = 0
  let leaseUntil = 120_000
  let inspected = 0
  const inspectedKeys = new Set<string>()
  harness.backend.headObject.mockImplementation(async (_bucket, key) => {
    inspectedKeys.add(key)
    if (++inspected % 8 === 0) elapsed += 5000
    if (!deleteEnabled) throw Object.assign(new Error('missing'), { code: 'ENOENT' })
    return { size: 12 }
  })
  const commit = harness.database.commitLifecycleAttempt.getMockImplementation()!
  harness.database.commitLifecycleAttempt.mockImplementation((input) =>
    elapsed >= leaseUntil ? undefined : commit(input)
  )
  const renew = harness.database.revalidateLifecycleRecoveryAttempt.getMockImplementation()!
  harness.database.revalidateLifecycleRecoveryAttempt.mockImplementation((...args) => {
    if (elapsed >= leaseUntil) return undefined
    leaseUntil = elapsed + 120_000
    return renew(...args)
  })
  harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValue({
    rawRowsExamined: 0,
    candidates: [],
    exhausted: true,
  })
  const executor = new NoncurrentLifecycleExecutor(harness.storage, {
    now: () => new Date(now.getTime() + elapsed),
    deleteEnabled,
    jobBudgetMs: 20_000,
    claimLeaseMs: 120_000,
    recoveryGraceMs: 1,
  })

  await expect(executor.run(coordinate, { tenantVersioningEnabled: true })).resolves.toMatchObject({
    status: deleteEnabled ? 'COMPLETED' : 'PAUSED',
  })
  if (deleteEnabled) {
    expect(harness.backend.headObject).not.toHaveBeenCalled()
    expect(harness.database.revalidateLifecycleRecoveryAttempt).toHaveBeenCalledOnce()
    expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledOnce()
    expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledWith(
      'root',
      initial.batch!.versions.flatMap((version) =>
        version.artifacts.map((artifact) => `tenant/bucket/${artifact.key}`)
      )
    )
    expect(harness.database.commitLifecycleAttempt).toHaveBeenCalledOnce()
    expect(
      harness.database.revalidateLifecycleRecoveryAttempt.mock.invocationCallOrder[0]
    ).toBeLessThan(harness.backend.deleteObjectsDetailed.mock.invocationCallOrder[0])
    expect(harness.getPersisted()?.batch).toBeUndefined()
    expect(harness.getPersisted()?.counters.objectVersionsDeleted).toBe(500)
    expect(harness.getPersisted()?.counters.bytesDeleted).toBe(6000)
  } else {
    expect(inspected).toBe(32)
    expect(harness.database.revalidateLifecycleRecoveryAttempt).toHaveBeenCalledTimes(4)
    expect(harness.database.commitLifecycleAttempt).toHaveBeenCalledTimes(4)
    expect(harness.getPersisted()?.counters.objectVersionsDeleted).toBe(16)
    await expect(
      executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'PAUSED' })
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    expect(harness.getPersisted()?.counters.objectVersionsDeleted).toBe(32)
    expect(harness.getPersisted()?.batch?.versions).toHaveLength(468)
    expect(inspected).toBe(64)
    expect(inspectedKeys.size).toBe(64)
    expect(harness.database.revalidateLifecycleRecoveryAttempt).toHaveBeenCalledTimes(8)
    expect(harness.database.commitLifecycleAttempt).toHaveBeenCalledTimes(8)
  }
})

describe('NoncurrentLifecycleExecutor', () => {
  test.each([
    { versioningEnabled: false, backend: 's3', canDelete: false },
    { versioningEnabled: true, backend: 's3', canDelete: true },
    { versioningEnabled: true, backend: 'file', canDelete: false },
  ])('derives deletion from versioning=$versioningEnabled on $backend', async (options) => {
    vi.resetModules()
    vi.stubEnv('STORAGE_VERSIONING_ENABLED', String(options.versioningEnabled))
    vi.stubEnv('STORAGE_BACKEND', options.backend)
    vi.stubEnv('STORAGE_S3_CLIENT_TIMEOUT', '5000')

    try {
      const { NoncurrentLifecycleExecutor: ConfiguredExecutor } = await import('./executor')
      const harness = createHarness({})
      const executor = new ConfiguredExecutor(harness.storage, { now: () => now })

      await expect(
        executor.run(coordinate, { tenantVersioningEnabled: true })
      ).resolves.toMatchObject({
        status: options.canDelete ? 'COMPLETED' : 'PAUSED',
      })
      expect(harness.database.evaluateNoncurrentLifecyclePage).toHaveBeenCalledTimes(
        options.canDelete ? 1 : 0
      )
      expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledTimes(options.canDelete ? 1 : 0)
    } finally {
      vi.unstubAllEnvs()
      vi.resetModules()
    }
  })

  test('rejects a job budget that can outlive its shard claim', () => {
    const storage = {
      db: { asSuperUser: () => ({}) },
    } as unknown as Storage

    expect(
      () =>
        new NoncurrentLifecycleExecutor(storage, {
          jobBudgetMs: 120_000,
          claimLeaseMs: 120_000,
        })
    ).toThrow('Lifecycle executor job budget must be shorter than its claim lease')
  })

  describe.each([
    { gate: 'fleet', deleteEnabled: false, tenantVersioningEnabled: true },
    { gate: 'tenant', deleteEnabled: true, tenantVersioningEnabled: false },
  ])('with the $gate deletion gate closed', ({ deleteEnabled, tenantVersioningEnabled }) => {
    it.each([
      'fresh',
      'cursor',
      'staged',
    ] as const)('pauses a %s run without scanning or changing its journal', async (progress) => {
      let continuation: LifecycleContinuation | undefined
      if (progress !== 'fresh') {
        const generation = randomUUID()
        const row = candidate(generation)
        continuation = createLifecycleContinuation({
          runId: randomUUID(),
          trigger: 'scheduled',
          generation,
          snapshotAt: now.toISOString(),
          topology: { scanKind: 'NONCURRENT', epoch: '1', shardId: 0, shardCount: 1 },
        })
        if (progress === 'staged') {
          continuation = stageLifecycleBatch(continuation, {
            rawRowsExamined: 1,
            pageEnd: { name: row.name, archivedAt: row.archivedAt },
            versions: [freezeLifecycleBatchVersion(row)],
          })
        } else {
          continuation.cursor = { name: row.name, archivedAt: row.archivedAt }
        }
      }
      const harness = createHarness({ deleteEnabled, continuation })

      await expect(harness.executor.run(coordinate, { tenantVersioningEnabled })).resolves.toEqual({
        status: 'PAUSED',
        runId: continuation?.runId,
      })

      expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledWith(
        expect.objectContaining({
          continuation: continuation ?? null,
          nextRunAt: new Date(now.getTime() + 1000).toISOString(),
        })
      )
      expect(harness.database.findLifecycleBucket).not.toHaveBeenCalled()
      expect(harness.database.saveLifecycleContinuation).not.toHaveBeenCalled()
      expect(harness.database.evaluateNoncurrentLifecyclePage).not.toHaveBeenCalled()
      expect(harness.database.findLifecycleObjectVersions).not.toHaveBeenCalled()
      expect(harness.database.armLifecycleAttempt).not.toHaveBeenCalled()
      expect(harness.database.completeLifecycleShardRun).not.toHaveBeenCalled()
      expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    })
  })

  it('persists an attempt before deleting exact data and info artifacts', async () => {
    const harness = createHarness({ deleteOutcome: 'DELETED' })
    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })

    expect(harness.calls.indexOf('arm')).toBeLessThan(harness.calls.indexOf('delete'))
    expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledWith('root', [
      `tenant/bucket/${harness.pageCandidate.name}/${harness.pageCandidate.version}`,
      `tenant/bucket/${harness.pageCandidate.name}/${harness.pageCandidate.version}.info`,
    ])
    expect(harness.calls).toContain('commit')
  })

  it('resumes an unarmed staged batch from raw PostgreSQL timestamp values', async () => {
    const generation = randomUUID()
    const stagedCandidate = candidate(generation)
    const initial = stageLifecycleBatch(
      createLifecycleContinuation({
        runId: randomUUID(),
        trigger: 'recovery',
        generation,
        snapshotAt: '2026-08-01T00:00:00.000Z',
        topology: {
          scanKind: 'NONCURRENT',
          epoch: '1',
          shardId: 0,
          shardCount: 1,
        },
      }),
      {
        rawRowsExamined: 1,
        pageEnd: {
          name: stagedCandidate.name,
          archivedAt: stagedCandidate.archivedAt,
        },
        versions: [freezeLifecycleBatchVersion(stagedCandidate)],
      }
    )
    const harness = createHarness({ continuation: initial })

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })

    expect(harness.database.findLifecycleObjectVersions).toHaveBeenCalledWith('bucket', [
      { name: stagedCandidate.name, version: stagedCandidate.version },
    ])
    expect(harness.calls.indexOf('arm')).toBeLessThan(harness.calls.indexOf('delete'))
    expect(harness.backend.deleteObjectsDetailed.mock.calls[0]?.[1]).toEqual([
      `tenant/bucket/${stagedCandidate.name}/${stagedCandidate.version}`,
      `tenant/bucket/${stagedCandidate.name}/${stagedCandidate.version}.info`,
    ])
  })

  it('drops a disappeared staged row and advances instead of wedging the shard', async () => {
    const generation = randomUUID()
    const stagedCandidate = candidate(generation)
    const initial = stageLifecycleBatch(
      createLifecycleContinuation({
        runId: randomUUID(),
        trigger: 'recovery',
        generation,
        snapshotAt: '2026-08-01T00:00:00.000Z',
        topology: {
          scanKind: 'NONCURRENT',
          epoch: '1',
          shardId: 0,
          shardCount: 1,
        },
      }),
      {
        rawRowsExamined: 1,
        pageEnd: {
          name: stagedCandidate.name,
          archivedAt: stagedCandidate.archivedAt,
        },
        versions: [freezeLifecycleBatchVersion(stagedCandidate)],
      }
    )
    const harness = createHarness({ continuation: initial })
    harness.database.findLifecycleObjectVersions.mockResolvedValue([])
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValue({
      rawRowsExamined: 0,
      candidates: [],
      exhausted: true,
    })
    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })

    expect(harness.database.armLifecycleAttempt).not.toHaveBeenCalled()
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    const filtered = harness.database.saveLifecycleContinuation.mock.calls[0][1]
    expect(filtered.batch).toBeUndefined()
    expect(filtered).toMatchObject({
      cursor: initial.pageEnd,
      counters: { batchesCompleted: 1 },
    })
  })

  it('reports LOST_CLAIM when an arm rejection cannot revalidate the claim', async () => {
    const generation = randomUUID()
    const stagedCandidate = candidate(generation)
    const initial = stageLifecycleBatch(
      createLifecycleContinuation({
        runId: randomUUID(),
        trigger: 'recovery',
        generation,
        snapshotAt: '2026-08-01T00:00:00.000Z',
        topology: {
          scanKind: 'NONCURRENT',
          epoch: '1',
          shardId: 0,
          shardCount: 1,
        },
      }),
      {
        rawRowsExamined: 1,
        pageEnd: {
          name: stagedCandidate.name,
          archivedAt: stagedCandidate.archivedAt,
        },
        versions: [freezeLifecycleBatchVersion(stagedCandidate)],
      }
    )
    const harness = createHarness({ continuation: initial })
    harness.database.armLifecycleAttempt.mockResolvedValueOnce(undefined)
    harness.database.revalidateLifecycleShardClaim.mockResolvedValueOnce(undefined)

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'LOST_CLAIM' })
    expect(harness.database.armLifecycleAttempt).toHaveBeenCalledTimes(1)
  })

  it('pauses when an arm rejection revalidates a claim whose policy changed', async () => {
    const generation = randomUUID()
    const stagedCandidate = candidate(generation)
    const initial = stageLifecycleBatch(
      createLifecycleContinuation({
        runId: randomUUID(),
        trigger: 'recovery',
        generation,
        snapshotAt: '2026-08-01T00:00:00.000Z',
        topology: {
          scanKind: 'NONCURRENT',
          epoch: '1',
          shardId: 0,
          shardCount: 1,
        },
      }),
      {
        rawRowsExamined: 1,
        pageEnd: {
          name: stagedCandidate.name,
          archivedAt: stagedCandidate.archivedAt,
        },
        versions: [freezeLifecycleBatchVersion(stagedCandidate)],
      }
    )
    const harness = createHarness({ continuation: initial })
    harness.database.armLifecycleAttempt.mockResolvedValueOnce(undefined)
    harness.database.revalidateLifecycleShardClaim.mockResolvedValueOnce(
      state(randomUUID(), initial)
    )

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'PAUSED' })
    expect(harness.database.armLifecycleAttempt).toHaveBeenCalledTimes(1)
    expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledWith(
      expect.objectContaining({ nextRunAt: expect.any(String) })
    )
  })

  it('throws a fenced failure when an arm rejection leaves the claim unchanged', async () => {
    const generation = randomUUID()
    const stagedCandidate = candidate(generation)
    const initial = stageLifecycleBatch(
      createLifecycleContinuation({
        runId: randomUUID(),
        trigger: 'recovery',
        generation,
        snapshotAt: '2026-08-01T00:00:00.000Z',
        topology: {
          scanKind: 'NONCURRENT',
          epoch: '1',
          shardId: 0,
          shardCount: 1,
        },
      }),
      {
        rawRowsExamined: 1,
        pageEnd: {
          name: stagedCandidate.name,
          archivedAt: stagedCandidate.archivedAt,
        },
        versions: [freezeLifecycleBatchVersion(stagedCandidate)],
      }
    )
    const harness = createHarness({ continuation: initial })
    harness.database.armLifecycleAttempt.mockResolvedValueOnce(undefined)

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).rejects.toThrow('Lifecycle arm fence rejected')
    expect(harness.database.findLifecycleObjectVersions).toHaveBeenCalledOnce()
    expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledWith(
      expect.objectContaining({ nextRunAt: expect.any(String), error: expect.any(Object) })
    )
  })

  it.each([
    true,
    false,
  ])('expires rule-eligible history without an upload-age delay with is_versioned=%s', async (isVersioned) => {
    const harness = createHarness({})
    harness.pageCandidate.isVersioned = isVersioned
    harness.pageCandidate.metadata = { size: 12 }
    harness.pageCandidate.createdAt = new Date(
      now.getTime() - 3 * 24 * 60 * 60 * 1000
    ).toISOString()
    harness.pageCandidate.archivedAt = new Date(
      now.getTime() - 2 * 24 * 60 * 60 * 1000
    ).toISOString()

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledOnce()
    expect(harness.database.armLifecycleAttempt).toHaveBeenCalledOnce()
  })

  it.each([
    null,
    {},
    { size: null },
    { size: -1 },
    { size: 1.5 },
    { size: 'invalid' },
  ])('does not let malformed size metadata %j block expiration', async (metadata) => {
    const harness = createHarness({})
    harness.pageCandidate.metadata = metadata
    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledOnce()
    expect(harness.database.armLifecycleAttempt).toHaveBeenCalledOnce()
  })

  it('arms a delete marker with null metadata and no backend artifacts', async () => {
    const harness = createHarness({})
    harness.pageCandidate.isDeleteMarker = true
    harness.pageCandidate.metadata = null
    harness.pageCandidate.createdAt = now.toISOString()

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.database.armLifecycleAttempt).toHaveBeenCalledOnce()
    expect(
      harness.database.armLifecycleAttempt.mock.calls[0][0].continuation.batch.versions
    ).toEqual([
      { name: harness.pageCandidate.name, version: harness.pageCandidate.version, artifacts: [] },
    ])
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  it.each([
    null,
    '856d94e5-5241-42ae-9c0b-d06332a7ae15',
  ])('deletes exactly the physical version %s for an unversioned data row', async (version) => {
    const harness = createHarness({})
    harness.pageCandidate.version = version
    harness.pageCandidate.isVersioned = false
    const name = harness.pageCandidate.name
    const key = version === null ? name : `${name}/${version}`

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.database.findLifecycleObjectVersions).toHaveBeenCalledWith(coordinate.bucketId, [
      { name, version },
    ])
    expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledWith('root', [
      `tenant/bucket/${key}`,
      `tenant/bucket/${key}.info`,
    ])
  })

  it('rejects a reloaded row whose physical version was omitted', async () => {
    const harness = createHarness({})
    harness.database.findLifecycleObjectVersions.mockImplementationOnce(() => [
      {
        id: harness.pageCandidate.id,
        name: harness.pageCandidate.name,
        archived_at: new Date(harness.pageCandidate.archivedAt),
      },
    ])

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).rejects.toThrow('A staged lifecycle candidate has no physical version identity')
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    expect(harness.database.armLifecycleAttempt).not.toHaveBeenCalled()
  })

  it('expires both older and younger eligible rows from the same page', async () => {
    const harness = createHarness({})
    const safe = harness.pageCandidate
    const young = {
      ...candidate(harness.generation),
      name: 'folder/young.txt',
      createdAt: new Date(now.getTime() - 3 * 24 * 60 * 60 * 1000).toISOString(),
      archivedAt: new Date(now.getTime() - 2 * 24 * 60 * 60 * 1000).toISOString(),
      metadata: {
        size: 12,
      },
    }
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValue({
      rawRowsExamined: 2,
      candidates: [safe, young],
      pageEnd: {
        name: young.name,
        archivedAt: young.archivedAt,
      },
      exhausted: true,
    })
    harness.database.findLifecycleObjectVersions.mockImplementation(
      (_bucketId: string, versions: Array<{ name: string; version: string | null }>) =>
        versions.map((version) => {
          const value = version.name === young.name ? young : safe
          return {
            id: value.id,
            bucket_id: value.bucketId,
            name: version.name,
            version: version.version,
            is_versioned: value.isVersioned,
            is_delete_marker: value.isDeleteMarker,
            metadata: value.metadata,
            created_at: new Date(value.createdAt),
            archived_at: new Date(value.archivedAt),
          }
        })
    )

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })

    expect(
      harness.database.armLifecycleAttempt.mock.calls[0][0].continuation.batch.versions
    ).toEqual([
      expect.objectContaining({ name: safe.name, version: safe.version }),
      expect.objectContaining({ name: young.name, version: young.version }),
    ])
    expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledWith(
      'root',
      expect.arrayContaining([
        `tenant/bucket/${safe.name}/${safe.version}`,
        `tenant/bucket/${safe.name}/${safe.version}.info`,
      ])
    )
    expect(harness.backend.deleteObjectsDetailed.mock.calls[0][1]).toEqual(
      expect.arrayContaining([expect.stringContaining(young.name)])
    )
  })

  it('treats a definitive per-key failure as resolved without deleting metadata', async () => {
    const harness = createHarness({ deleteOutcome: 'FAILED' })
    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })

    const committed = harness.database.commitLifecycleAttempt.mock.calls[0][0]
    expect(committed.continuation.batch.versions[0].artifacts).toEqual([
      expect.objectContaining({ outcome: 'FAILED' }),
      expect.objectContaining({ outcome: 'FAILED' }),
    ])
  })

  it('keeps partial per-version deletion in-flight and retries the failed artifact', async () => {
    const harness = createHarness({
      deleteOutcomesByCall: [['DELETED', 'FAILED'], ['DELETED']],
    })

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'PARTIAL' })
    expect(harness.getPersisted()).toMatchObject({
      batch: {
        inFlight: expect.any(Object),
        versions: [
          {
            artifacts: [
              expect.objectContaining({ outcome: 'DELETED' }),
              expect.objectContaining({ outcome: 'FAILED' }),
            ],
          },
        ],
      },
    })

    harness.getPersisted()!.batch!.inFlight!.startedAt = '2026-08-14T00:00:00.000Z'
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValue({
      rawRowsExamined: 0,
      candidates: [],
      exhausted: true,
    })
    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.backend.deleteObjectsDetailed.mock.calls[1][1]).toEqual([
      `tenant/bucket/${harness.pageCandidate.name}/${harness.pageCandidate.version}.info`,
    ])
    expect(harness.backend.headObject).not.toHaveBeenCalled()
    expect(harness.database.revalidateLifecycleRecoveryAttempt).toHaveBeenCalledOnce()
    expect(harness.database.commitLifecycleAttempt).toHaveBeenCalledTimes(2)
  })

  it('backs off repeated incomplete deletion and records the backend failure', async () => {
    const harness = createHarness({
      deleteOutcomesByCall: [['DELETED', 'FAILED'], ['FAILED']],
    })
    for (const delayMinutes of [5, 10]) {
      await expect(
        harness.executor.run(coordinate, { tenantVersioningEnabled: true })
      ).resolves.toMatchObject({ status: 'PARTIAL' })
      expect(harness.database.releaseLifecycleShardClaim).toHaveBeenLastCalledWith(
        expect.objectContaining({
          continuation: harness.getPersisted(),
          nextRunAt: new Date(now.getTime() + delayMinutes * 60_000).toISOString(),
          error: expect.objectContaining({ message: 'failed', pendingArtifacts: 1 }),
        })
      )
      harness.getPersisted()!.batch!.inFlight!.startedAt = '2026-08-14T00:00:00.000Z'
    }
    expect(harness.backend.deleteObjectsDetailed.mock.calls[1][1]).toEqual([
      `tenant/bucket/${harness.pageCandidate.name}/${harness.pageCandidate.version}.info`,
    ])
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
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'PARTIAL' })
    const continuation = harness.getPersisted()!
    if (!waitForGrace) {
      continuation.batch!.inFlight!.startedAt = '2026-08-14T00:00:00.000Z'
      harness.database.revalidateLifecycleRecoveryAttempt.mockResolvedValueOnce(undefined)
    }
    harness.backend.deleteObjectsDetailed.mockClear()
    harness.database.commitLifecycleAttempt.mockClear()

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status })
    expect(harness.database.revalidateLifecycleRecoveryAttempt).toHaveBeenCalledTimes(
      waitForGrace ? 0 : 1
    )
    expect(harness.backend.headObject).not.toHaveBeenCalled()
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    expect(harness.database.commitLifecycleAttempt).not.toHaveBeenCalled()
    expect(harness.getPersisted()).toBe(continuation)
  })

  it('recovers partial legacy deletion using the durable null identity and unsuffixed sidecar', async () => {
    const harness = createHarness({
      deleteOutcomesByCall: [['DELETED', 'FAILED'], ['DELETED']],
    })
    harness.pageCandidate.version = null
    harness.pageCandidate.isVersioned = false
    const name = harness.pageCandidate.name

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'PARTIAL' })
    expect(harness.getPersisted()?.batch?.versions).toEqual([
      {
        name,
        version: null,
        artifacts: [
          { key: name, outcome: 'DELETED' },
          { key: `${name}.info`, outcome: 'FAILED', error: 'failed' },
        ],
      },
    ])

    harness.getPersisted()!.batch!.inFlight!.startedAt = '2026-08-14T00:00:00.000Z'
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValue({
      rawRowsExamined: 0,
      candidates: [],
      exhausted: true,
    })
    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.backend.deleteObjectsDetailed.mock.calls[1][1]).toEqual([
      `tenant/bucket/${name}.info`,
    ])
    expect(harness.getPersisted()?.batch).toBeUndefined()
  })

  it('keeps an unknown backend outcome in-flight and leaves the shard due', async () => {
    const harness = createHarness({ deleteOutcome: 'UNKNOWN' })
    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled: true })
    ).resolves.toMatchObject({ status: 'PARTIAL' })

    expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledWith(
      expect.objectContaining({
        continuation: expect.objectContaining({
          batch: expect.objectContaining({ inFlight: expect.any(Object) }),
        }),
        nextRunAt: expect.any(String),
      })
    )
    expect(harness.database.completeLifecycleShardRun).not.toHaveBeenCalled()
  })

  it('preserves committed recovery progress when the commit acknowledgement is lost', async () => {
    const harness = createHarness({})
    const commit = harness.database.commitLifecycleAttempt.getMockImplementation()!
    const failure = new Error('connection lost after COMMIT')
    harness.database.commitLifecycleAttempt.mockImplementationOnce(async (input) => {
      commit(input)
      throw failure
    })
    harness.database.releaseLifecycleShardClaim.mockImplementation(async (input) => {
      if (input.continuation !== undefined) {
        await harness.database.saveLifecycleContinuation(input, input.continuation)
      }
      return true
    })

    await expect(harness.executor.run(coordinate, { tenantVersioningEnabled: true })).rejects.toBe(
      failure
    )

    expect(harness.getPersisted()).toMatchObject({
      counters: { objectVersionsDeleted: 1, bytesDeleted: 12, batchesCompleted: 1 },
    })
    expect(harness.getPersisted()!.batch).toBeUndefined()
    expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledWith(
      expect.objectContaining({
        nextRunAt: expect.any(String),
        error: { message: failure.message, name: 'Error' },
      })
    )
  })

  it.each([
    { deleteEnabled: false, tenantVersioningEnabled: true, headMissing: false },
    { deleteEnabled: false, tenantVersioningEnabled: true, headMissing: true },
    { deleteEnabled: true, tenantVersioningEnabled: false, headMissing: false },
    { deleteEnabled: true, tenantVersioningEnabled: false, headMissing: true },
  ])('reconciles an armed journal without redriving deletion: %o', async ({
    deleteEnabled,
    tenantVersioningEnabled,
    headMissing,
  }) => {
    const generation = randomUUID()
    const pageCandidate = candidate(generation)
    const initial = stageLifecycleBatch(
      createLifecycleContinuation({
        runId: randomUUID(),
        trigger: 'recovery',
        generation,
        snapshotAt: '2026-08-01T00:00:00.000Z',
        topology: {
          scanKind: 'NONCURRENT',
          epoch: '1',
          shardId: 0,
          shardCount: 1,
        },
      }),
      {
        rawRowsExamined: 1,
        pageEnd: {
          name: pageCandidate.name,
          archivedAt: pageCandidate.archivedAt,
        },
        versions: [
          {
            name: pageCandidate.name,
            version: pageCandidate.version,
            artifacts: [
              { key: `${pageCandidate.name}/${pageCandidate.version}`, outcome: 'UNRESOLVED' },
              { key: `${pageCandidate.name}/${pageCandidate.version}.info`, outcome: 'UNRESOLVED' },
            ],
          },
        ],
      }
    )
    initial.batch!.inFlight = {
      attemptId: randomUUID(),
      authorizedGeneration: generation,
      startedAt: '2026-08-01T00:00:00.000Z',
    }
    const harness = createHarness({ continuation: initial, deleteEnabled, headMissing })

    await expect(
      harness.executor.run(coordinate, { tenantVersioningEnabled })
    ).resolves.toMatchObject({
      status: 'PAUSED',
    })
    expect(harness.backend.headObject).toHaveBeenCalledTimes(2)
    for (const suffix of ['', '.info']) {
      expect(harness.backend.headObject).toHaveBeenCalledWith(
        'root',
        `tenant/bucket/${pageCandidate.name}/${pageCandidate.version}${suffix}`,
        undefined,
        { confirmMissing: true, signal: expect.any(AbortSignal) }
      )
    }
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    expect(harness.database.evaluateNoncurrentLifecyclePage).not.toHaveBeenCalled()
    expect(harness.database.armLifecycleAttempt).not.toHaveBeenCalled()
    expect(harness.database.revalidateLifecycleRecoveryAttempt).toHaveBeenCalledOnce()
    expect(harness.database.completeLifecycleShardRun).not.toHaveBeenCalled()
    expect(harness.database.releaseLifecycleShardClaim).toHaveBeenCalledWith(
      expect.objectContaining({
        continuation: harness.getPersisted(),
        nextRunAt: expect.any(String),
      })
    )
    if (headMissing) {
      expect(harness.getPersisted()?.batch).toBeUndefined()
      expect(harness.getPersisted()?.counters.objectVersionsDeleted).toBe(1)
    } else {
      expect(harness.getPersisted()?.batch?.inFlight).toEqual(initial.batch!.inFlight)
    }
    expect(harness.database.commitLifecycleAttempt.mock.calls[0][0].continuation).toMatchObject({
      batch: {
        versions: [
          {
            artifacts: [
              expect.objectContaining({ outcome: headMissing ? 'ABSENT' : 'UNRESOLVED' }),
              expect.objectContaining({ outcome: headMissing ? 'ABSENT' : 'UNRESOLVED' }),
            ],
          },
        ],
      },
    })
  })
})
