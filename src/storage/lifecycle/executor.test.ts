import { randomUUID } from 'node:crypto'
import { ERRORS } from '@internal/errors'
import { logSchema } from '@internal/monitoring'
import {
  lifecycleBytesDeleted,
  lifecycleDeleteMarkersDeleted,
  lifecycleObjectVersionsDeleted,
  lifecycleVersionsEligible,
  lifecycleVersionsExamined,
} from '@internal/monitoring/metrics'
import { newContinuation } from '../../test/utils/lifecycle-continuation'
import type { Database } from '../database'
import type {
  LifecycleBatch,
  LifecycleBucket,
  LifecycleContinuation,
  LifecycleEvaluatedCandidate,
  LifecycleState,
} from '../schemas'
import type { Storage } from '../storage'
import { advanceLifecycleBatch } from './continuation'
import { evaluationJitterMs, lifecycleErrorMessage, NoncurrentLifecycleExecutor } from './executor'

vi.mock('@internal/monitoring/metrics', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@internal/monitoring/metrics')>()),
  lifecycleBytesDeleted: { add: vi.fn() },
  lifecycleDeleteMarkersDeleted: { add: vi.fn() },
  lifecycleObjectVersionsDeleted: { add: vi.fn() },
  lifecycleVersionsEligible: { add: vi.fn() },
  lifecycleVersionsExamined: { add: vi.fn() },
}))

const DAY_MS = 24 * 60 * 60 * 1000
const now = new Date('2026-08-15T00:00:00.000Z')
const pageEnd = { name: 'folder/object.txt', archivedAt: '2026-07-02T00:00:00.000Z' }
const unfinishedPage = {
  rawRowsExamined: 1,
  candidates: [],
  pageEnd: { name: 'folder/skipped.txt', archivedAt: '2026-07-02T00:00:00.000Z' },
  exhausted: false,
}
const emptyPage = { rawRowsExamined: 0, candidates: [], exhausted: true }

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
  }
}

function candidate(): LifecycleEvaluatedCandidate {
  return {
    id: randomUUID(),
    name: 'folder/object.txt',
    version: randomUUID(),
    isDeleteMarker: false,
    archivedAt: '2026-07-02T00:00:00.000Z',
    rawRowNumber: 1,
  }
}

function fullPage(count: number) {
  const candidates = Array.from({ length: count }, (_, index) => ({
    ...candidate(),
    name: `object-${String(index).padStart(2, '0')}`,
    rawRowNumber: index + 1,
  }))
  return {
    rawRowsExamined: candidates.length,
    candidates,
    pageEnd: { name: candidates.at(-1)!.name, archivedAt: candidates.at(-1)!.archivedAt },
    exhausted: true,
  }
}

function state(continuation: LifecycleContinuation | null = null): LifecycleState {
  return {
    bucketId: 'bucket',
    claimId: randomUUID(),
    continuation,
    failureCount: 0,
  }
}

function outboxOf(batch: LifecycleBatch) {
  return batch.versions.flatMap(({ name, version, artifacts }) =>
    artifacts.map((key) => ({ bucketId: 'bucket', key, name, version }))
  )
}

function createHarness(input: {
  continuation?: LifecycleContinuation
  deleteOutcome?: 'DELETED' | 'FAILED' | 'UNKNOWN'
  failureCount?: number
  jobBudgetMs?: number
  now?: () => Date
}) {
  const generation = input.continuation?.generation ?? randomUUID()
  let persisted = input.continuation ?? null
  let bucket: LifecycleBucket = configuration(generation)
  const claimed = state(persisted)
  claimed.failureCount = input.failureCount ?? 0
  const pageCandidate = candidate()
  const calls: string[] = []
  const database = {
    tenantId: 'tenant',
    hasMigration: vi.fn().mockResolvedValue(true),
    asSuperUser: vi.fn(() => database),
    claimLifecycleBucket: vi
      .fn()
      .mockImplementation(() => ({ ...claimed, continuation: persisted })),
    findLifecycleBucket: vi.fn().mockImplementation(() => bucket),
    evaluateNoncurrentLifecyclePage: vi.fn().mockResolvedValue({
      rawRowsExamined: 1,
      candidates: [pageCandidate],
      pageEnd: {
        name: pageCandidate.name,
        archivedAt: pageCandidate.archivedAt,
      },
      exhausted: true,
    }),
    armLifecycleAttempt: vi
      .fn()
      .mockImplementation(
        (attempt: { continuation: LifecycleContinuation; batch: LifecycleBatch }) => {
          const objectVersions = attempt.batch.versions.filter(
            (version) => version.artifacts.length > 0
          ).length
          persisted = advanceLifecycleBatch(attempt.continuation, attempt.batch, {
            objectVersions,
            deleteMarkers: attempt.batch.versions.length - objectVersions,
            bytes: objectVersions * 12,
          })
          calls.push('arm')
          return {
            status: 'armed' as const,
            state: { ...claimed, continuation: persisted },
            artifacts: outboxOf(attempt.batch),
          }
        }
      ),
    revalidateLifecycleClaim: vi.fn().mockImplementation(() => ({
      ...claimed,
      continuation: persisted,
    })),
    resolveLifecycleAbandonedArtifacts: vi.fn().mockImplementation(() => {
      calls.push('resolve')
    }),
    releaseLifecycleClaim: vi.fn().mockImplementation((input) => {
      if (input.continuation !== undefined) persisted = input.continuation
      if (input.error) claimed.failureCount++
      return true
    }),
    completeLifecycleRun: vi.fn().mockImplementation(() => {
      calls.push('complete')
      return true
    }),
  }
  const backend = {
    deleteObjectsDetailed: vi.fn().mockImplementation((_bucket: string, keys: string[]) => {
      calls.push('delete')
      return keys.map((key) => {
        const outcome = input.deleteOutcome ?? 'DELETED'
        return {
          key,
          outcome,
          ...(outcome === 'DELETED' ? {} : { error: { message: outcome.toLowerCase() } }),
        }
      })
    }),
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
    now: input.now ?? (() => now),
    pageSize: 480,
    jobBudgetMs: input.jobBudgetMs ?? 20_000,
    claimLeaseMs: 120_000,
    fairnessDelayMs: 1000,
  })
  return {
    backend,
    calls,
    database,
    executor,
    run: () => executor.run(bucketId, { tenantLifecycleEnabled: true }),
    generation,
    pageCandidate,
    setBucket: (next: LifecycleBucket) => {
      bucket = next
    },
    getPersisted: () => persisted,
  }
}

const bucketId = 'bucket'

test.each([false, true])(
  'preserves a claim decoding failure when release fails=%s',
  async (releaseFails) => {
    const harness = createHarness({})
    const failure = new TypeError('Unsupported lifecycle continuation version')
    const releaseFailure = new Error('database offline')
    harness.database.claimLifecycleBucket.mockRejectedValueOnce(failure)
    if (releaseFails) harness.database.releaseLifecycleClaim.mockRejectedValueOnce(releaseFailure)
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})

    await expect(harness.run()).rejects.toBe(failure)
    expect(harness.database.releaseLifecycleClaim).toHaveBeenCalledWith(
      expect.objectContaining({
        bucketId,
        claimId: expect.any(String),
        nextRunAt: expect.any(String),
        error: { name: 'TypeError', message: failure.message },
      })
    )
    expect(harness.database.releaseLifecycleClaim.mock.calls[0][0]).not.toHaveProperty(
      'continuation'
    )
    if (releaseFails) {
      expect(warning).toHaveBeenCalledWith(
        expect.anything(),
        expect.stringContaining('release lifecycle claim'),
        expect.objectContaining({
          error: releaseFailure,
          tenantId: 'tenant',
          metadata: JSON.stringify({
            bucketId,
            claimId: harness.database.releaseLifecycleClaim.mock.calls[0][0].claimId,
          }),
        })
      )
    } else {
      expect(warning).not.toHaveBeenCalled()
    }
  }
)

it('truncates error messages without splitting a surrogate pair', () => {
  const message = lifecycleErrorMessage(new Error(`${'a'.repeat(999)}😀tail`))
  expect(message.isWellFormed()).toBe(true)
  expect(message.length).toBeLessThanOrEqual(1000)
})

describe('NoncurrentLifecycleExecutor', () => {
  it('counts a committed deletion once even when the backend deletion fails', async () => {
    const versions = vi.spyOn(lifecycleObjectVersionsDeleted, 'add')
    const bytes = vi.spyOn(lifecycleBytesDeleted, 'add')
    const harness = createHarness({ deleteOutcome: 'FAILED' })

    await harness.run()

    expect(versions).toHaveBeenCalledExactlyOnceWith(1)
    expect(bytes).toHaveBeenCalledExactlyOnceWith(12)
  })

  it('does not export a deletion when its metadata commit is not acknowledged', async () => {
    const versions = vi.spyOn(lifecycleObjectVersionsDeleted, 'add')
    const bytes = vi.spyOn(lifecycleBytesDeleted, 'add')
    const harness = createHarness({})
    harness.database.armLifecycleAttempt.mockRejectedValueOnce(new Error('lost acknowledgement'))
    await expect(harness.run()).rejects.toThrow('lost acknowledgement')
    expect(versions).not.toHaveBeenCalled()
    expect(bytes).not.toHaveBeenCalled()
  })

  it('exports only the arm delta of a delete marker after earlier batches', async () => {
    const initial = newContinuation(randomUUID())
    initial.counters.objectVersionsDeleted = 5
    initial.counters.deleteMarkersDeleted = 3
    initial.counters.bytesDeleted = 60
    const harness = createHarness({ continuation: initial })
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValue({
      rawRowsExamined: 1,
      candidates: [{ ...candidate(), isDeleteMarker: true }],
      pageEnd,
      exhausted: true,
    })

    await harness.run()

    expect(lifecycleObjectVersionsDeleted.add).toHaveBeenCalledExactlyOnceWith(0)
    expect(lifecycleDeleteMarkersDeleted.add).toHaveBeenCalledExactlyOnceWith(1)
    expect(lifecycleBytesDeleted.add).toHaveBeenCalledExactlyOnceWith(0)
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    expect(harness.database.resolveLifecycleAbandonedArtifacts).not.toHaveBeenCalled()
  })

  test('does not claim work before writer coordination is installed', async () => {
    const harness = createHarness({})
    harness.database.hasMigration.mockResolvedValue(false)
    await expect(harness.run()).resolves.toEqual({ status: 'PAUSED' })
    expect(harness.database.hasMigration).toHaveBeenCalledExactlyOnceWith(
      'lifecycle-writer-coordination'
    )
    expect(harness.database.claimLifecycleBucket).not.toHaveBeenCalled()
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  it.each([
    { state: 'versioning disabled', change: { versioning_status: 'DISABLED' as const } },
    { state: 'no configuration', change: { lifecycle_configuration: null } },
    { state: 'no generation', change: { lifecycle_configuration_generation: null } },
  ])('pauses without evaluating when the claimed bucket has $state', async ({ change }) => {
    const continuation = newContinuation(randomUUID())
    const harness = createHarness({ continuation })
    harness.setBucket({ ...configuration(harness.generation), ...change })

    await expect(harness.run()).resolves.toEqual({ status: 'PAUSED', runId: continuation.runId })
    expect(harness.database.releaseLifecycleClaim).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ continuation: null, nextRunAt: null })
    )

    expect(harness.database.evaluateNoncurrentLifecyclePage).not.toHaveBeenCalled()
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  it('pauses under the closed tenant gate without changing its continuation', async () => {
    const continuation = { ...newContinuation(randomUUID()), cursor: pageEnd }
    const harness = createHarness({ continuation })

    await expect(
      harness.executor.run(bucketId, { tenantLifecycleEnabled: false })
    ).resolves.toEqual({
      status: 'PAUSED',
      runId: continuation.runId,
    })

    expect(harness.database.releaseLifecycleClaim).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        continuation,
        nextRunAt: new Date(now.getTime() + 1000).toISOString(),
      })
    )
    expect(harness.database.findLifecycleBucket).not.toHaveBeenCalled()
    expect(harness.database.evaluateNoncurrentLifecyclePage).not.toHaveBeenCalled()
    expect(harness.database.armLifecycleAttempt).not.toHaveBeenCalled()
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  it('schedules the next evaluation a day out plus a deterministic jitter below a tenth of a day', async () => {
    const harness = createHarness({})

    await harness.run()

    const [identity, result] = harness.database.completeLifecycleRun.mock.calls[0]
    expect(identity.generation).toBe(result.generation)
    expect(identity.generation).toBeTruthy()
    const nextRunAt = new Date(harness.database.completeLifecycleRun.mock.calls[0][2]).getTime()
    const day = 24 * 60 * 60 * 1000
    expect(nextRunAt).toBeGreaterThan(now.getTime() + day)
    expect(nextRunAt).toBeLessThan(now.getTime() + day + day / 10)
  })

  it('completes with the run identity and counters and schedules from the completion clock', async () => {
    let clock = now.getTime()
    const harness = createHarness({ now: () => new Date(clock) })
    harness.backend.deleteObjectsDetailed.mockImplementationOnce((_bucket, keys: string[]) => {
      clock += 3000
      return keys.map((key) => ({ key, outcome: 'DELETED' }))
    })

    await expect(harness.run()).resolves.toMatchObject({
      status: 'COMPLETED',
      runId: expect.any(String),
    })

    const armed = harness.getPersisted()!
    const [identity, result, nextRunAt] = harness.database.completeLifecycleRun.mock.calls[0]
    expect(identity).toEqual({
      bucketId,
      claimId: harness.database.claimLifecycleBucket.mock.calls[0][0].claimId,
      generation: armed.generation,
    })
    expect(harness.database.armLifecycleAttempt.mock.calls[0][0].claimId).toBe(identity.claimId)
    expect(result).toEqual({
      runId: armed.runId,
      generation: armed.generation,
      snapshotAt: now.toISOString(),
      counters: armed.counters,
      completedAt: new Date(clock).toISOString(),
    })
    expect(nextRunAt).toBe(
      new Date(clock + DAY_MS + evaluationJitterMs(`tenant/${bucketId}`)).toISOString()
    )
  })

  it('reports NOT_CLAIMED without releasing when nothing can be claimed', async () => {
    const harness = createHarness({})
    harness.database.claimLifecycleBucket.mockResolvedValueOnce(undefined)

    await expect(harness.run()).resolves.toEqual({ status: 'NOT_CLAIMED' })
    expect(harness.database.claimLifecycleBucket).toHaveBeenCalledExactlyOnceWith({
      bucketId,
      claimId: expect.any(String),
      leaseMs: 120_000,
    })
    expect(harness.database.releaseLifecycleClaim).not.toHaveBeenCalled()
  })

  it('reports a lost claim without releasing when the bucket was deleted mid-run', async () => {
    const harness = createHarness({})
    harness.database.findLifecycleBucket.mockRejectedValueOnce(ERRORS.NoSuchBucket(bucketId))

    await expect(harness.run()).resolves.toEqual({ status: 'LOST_CLAIM' })
    expect(harness.database.releaseLifecycleClaim).not.toHaveBeenCalled()
  })

  it('caps the persisted backend delete error', async () => {
    const harness = createHarness({ deleteOutcome: 'FAILED' })
    harness.backend.deleteObjectsDetailed.mockImplementation((_bucket: string, keys: string[]) =>
      keys.map((key) => ({ key, outcome: 'FAILED', error: { message: 'x'.repeat(5000) } }))
    )

    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})

    await harness.run()

    expect(harness.database.releaseLifecycleClaim.mock.calls[0][0].error.message).toHaveLength(1000)
    expect(warning.mock.calls[0][2]).toMatchObject({ tenantId: 'tenant', project: 'tenant' })
    const logged = JSON.parse(warning.mock.calls[0][2].metadata as string)
    expect(logged.error).toHaveLength(1000)
    expect(logged).toMatchObject({ bucketId, failedArtifacts: 2 })
  })

  it('reports an ineligible batch without revalidation or backend deletion', async () => {
    const harness = createHarness({})
    harness.database.armLifecycleAttempt.mockResolvedValueOnce({ status: 'ineligible' })

    await expect(harness.run()).resolves.toMatchObject({
      status: 'INELIGIBLE',
      runId: expect.any(String),
    })
    expect(harness.database.revalidateLifecycleClaim).not.toHaveBeenCalled()
    expect(harness.database.releaseLifecycleClaim).not.toHaveBeenCalled()
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  it.each(['lost', 'unchanged'] as const)(
    'handles an arm rejection with a %s claim',
    async (claim) => {
      const harness = createHarness({})
      harness.database.armLifecycleAttempt.mockResolvedValueOnce({ status: 'fenced' })
      if (claim === 'lost')
        harness.database.revalidateLifecycleClaim.mockResolvedValueOnce(undefined)

      const run = harness.run()
      if (claim === 'unchanged') {
        await expect(run).rejects.toThrow('Lifecycle arm fence rejected')
      } else {
        await expect(run).resolves.toMatchObject({
          status: 'LOST_CLAIM',
          runId: expect.any(String),
        })
      }
      expect(harness.database.armLifecycleAttempt).toHaveBeenCalledOnce()
      expect(harness.database.revalidateLifecycleClaim).toHaveBeenCalledOnce()
      expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
      if (claim === 'lost') {
        expect(harness.database.releaseLifecycleClaim).not.toHaveBeenCalled()
      } else {
        expect(harness.database.releaseLifecycleClaim).toHaveBeenCalledWith(
          expect.objectContaining({
            nextRunAt: expect.any(String),
            error: expect.any(Object),
          })
        )
      }
    }
  )

  it('restarts from a fresh continuation over a run of a changed configuration generation', async () => {
    const initial = { ...newContinuation(randomUUID()), cursor: pageEnd }
    const harness = createHarness({ continuation: initial })
    const generation = randomUUID()
    harness.setBucket(configuration(generation))

    await expect(harness.run()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.database.findLifecycleBucket).toHaveBeenCalledOnce()
    expect(harness.database.evaluateNoncurrentLifecyclePage.mock.calls[0][0].cursor).toBeUndefined()
    const armed = harness.database.armLifecycleAttempt.mock.calls[0][0].continuation
    expect(armed.generation).toBe(generation)
    expect(armed.runId).not.toBe(initial.runId)
    expect(armed.snapshotAt).toBe(now.toISOString())
    expect(harness.database.evaluateNoncurrentLifecyclePage.mock.calls[0][0].snapshotAt).toBe(
      now.toISOString()
    )
  })

  it('pauses instead of failing when the configuration is regenerated between evaluation and arm', async () => {
    const harness = createHarness({})
    const bucket = configuration(harness.generation)
    harness.setBucket(bucket)
    harness.database.findLifecycleBucket.mockResolvedValueOnce(bucket).mockResolvedValueOnce({
      ...bucket,
      lifecycle_configuration_generation: randomUUID(),
    })
    harness.database.armLifecycleAttempt.mockResolvedValueOnce({ status: 'fenced' })

    await expect(harness.run()).resolves.toEqual({
      status: 'PAUSED',
      runId: expect.any(String),
    })
    expect(harness.database.findLifecycleBucket).toHaveBeenCalledTimes(2)
    expect(harness.database.releaseLifecycleClaim).toHaveBeenCalledOnce()
    expect(harness.database.releaseLifecycleClaim.mock.calls[0][0]).not.toHaveProperty('error')
    expect(harness.database.releaseLifecycleClaim.mock.calls[0][0].continuation).toBeNull()
    expect(harness.database.releaseLifecycleClaim.mock.calls[0][0].nextRunAt).toBe(
      new Date(now.getTime() + 1000).toISOString()
    )
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  it('bounds the backend delete and arms with the claim lease', async () => {
    const timeout = vi.spyOn(AbortSignal, 'timeout')
    const harness = createHarness({})

    await harness.run()

    expect(timeout).toHaveBeenCalledWith(120_000)
    expect(harness.database.armLifecycleAttempt.mock.calls[0][0].leaseMs).toBe(120_000)
  })

  it('records the examined and eligible versions of each evaluated page', async () => {
    const harness = createHarness({})
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValueOnce({
      ...fullPage(1),
      rawRowsExamined: 5,
    })

    await harness.run()

    expect(lifecycleVersionsExamined.add).toHaveBeenCalledExactlyOnceWith(5)
    expect(lifecycleVersionsEligible.add).toHaveBeenCalledExactlyOnceWith(1)
  })

  it('stamps completion with the clock at completion rather than the snapshot', async () => {
    let tick = 0
    const harness = createHarness({
      now: () => new Date(now.getTime() + tick++ * 1000),
    })

    await harness.run()

    const completion = harness.database.completeLifecycleRun.mock.calls[0][1]
    expect(Date.parse(completion.completedAt)).toBeGreaterThan(Date.parse(completion.snapshotAt))
  })

  it('releases idle without scanning when no rule compiles', async () => {
    const continuation = newContinuation(randomUUID())
    const harness = createHarness({ continuation })
    const bucket = configuration(harness.generation)
    harness.setBucket({
      ...bucket,
      lifecycle_configuration: {
        rules: [{ ...bucket.lifecycle_configuration.rules[0], status: 'Disabled' as const }],
      },
    })

    await expect(harness.run()).resolves.toEqual({ status: 'PAUSED', runId: continuation.runId })
    expect(harness.database.releaseLifecycleClaim).toHaveBeenCalledExactlyOnceWith(
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

    await expect(harness.run()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.database.evaluateNoncurrentLifecyclePage).toHaveBeenCalledTimes(2)
    expect(harness.database.evaluateNoncurrentLifecyclePage.mock.calls[1][0].cursor).toEqual(
      pageEnd
    )
  })

  it('sends only the artifacts the arm returns when it drops vetoed versions', async () => {
    const harness = createHarness({})
    const kept = harness.pageCandidate
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValueOnce({
      rawRowsExamined: 2,
      candidates: [{ ...candidate(), name: 'folder/vetoed.txt' }, kept],
      pageEnd: { name: kept.name, archivedAt: kept.archivedAt },
      exhausted: true,
    })
    harness.database.armLifecycleAttempt.mockImplementationOnce(({ continuation, batch }) => ({
      status: 'armed',
      state: { ...state(), continuation },
      artifacts: outboxOf({ ...batch, versions: batch.versions.slice(1) }),
    }))

    await expect(harness.run()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledExactlyOnceWith(
      'root',
      [
        `tenant/bucket/${kept.name}/${kept.version}`,
        `tenant/bucket/${kept.name}/${kept.version}.info`,
      ],
      { signal: expect.any(AbortSignal) }
    )
  })

  it('continues with the next page without a backend call when the arm deletes nothing', async () => {
    const harness = createHarness({})
    harness.database.evaluateNoncurrentLifecyclePage
      .mockResolvedValueOnce({
        rawRowsExamined: 1,
        candidates: [harness.pageCandidate],
        pageEnd,
        exhausted: false,
      })
      .mockResolvedValueOnce(emptyPage)
    harness.database.armLifecycleAttempt.mockImplementationOnce(({ continuation, batch }) => ({
      status: 'armed',
      state: {
        ...state(),
        continuation: advanceLifecycleBatch(continuation, batch, {
          objectVersions: 0,
          deleteMarkers: 0,
          bytes: 0,
        }),
      },
      artifacts: [],
    }))

    await expect(harness.run()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    expect(harness.database.resolveLifecycleAbandonedArtifacts).not.toHaveBeenCalled()
    expect(harness.database.evaluateNoncurrentLifecyclePage).toHaveBeenCalledTimes(2)
    expect(harness.database.evaluateNoncurrentLifecyclePage.mock.calls[1][0].cursor).toEqual(
      pageEnd
    )
  })

  it('reports a lost claim when completing the run is rejected', async () => {
    const harness = createHarness({})
    harness.database.completeLifecycleRun.mockResolvedValueOnce(false)

    await expect(harness.run()).resolves.toMatchObject({ status: 'LOST_CLAIM' })
  })

  it.each(['idle', 'tenant gate', 'budget', 'incomplete deletion'] as const)(
    'reports a lost claim when release fails during %s',
    async (path) => {
      const harness = createHarness({
        jobBudgetMs: path === 'budget' ? 0 : undefined,
        deleteOutcome: path === 'incomplete deletion' ? 'FAILED' : undefined,
      })
      if (path === 'idle') {
        harness.setBucket({ ...configuration(harness.generation), lifecycle_configuration: null })
      }
      if (path === 'budget') {
        harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValueOnce(unfinishedPage)
      }
      harness.database.releaseLifecycleClaim.mockResolvedValueOnce(false)

      await expect(
        harness.executor.run(bucketId, { tenantLifecycleEnabled: path !== 'tenant gate' })
      ).resolves.toMatchObject({ status: 'LOST_CLAIM' })
    }
  )

  it('releases a run that exhausts its job budget after the fairness delay', async () => {
    const harness = createHarness({ jobBudgetMs: 0 })
    harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValueOnce(unfinishedPage)

    await expect(harness.run()).resolves.toMatchObject({
      status: 'PARTIAL',
      runId: expect.any(String),
    })
    expect(harness.database.releaseLifecycleClaim).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        continuation: expect.objectContaining({ cursor: unfinishedPage.pageEnd }),
        nextRunAt: new Date(now.getTime() + 1000).toISOString(),
      })
    )
    expect(harness.database.evaluateNoncurrentLifecyclePage).toHaveBeenCalledOnce()
  })

  it('processes the first batch when evaluation consumes the remaining budget', async () => {
    let clock = now.getTime()
    const harness = createHarness({ jobBudgetMs: 1000, now: () => new Date(clock) })
    const evaluate = harness.database.evaluateNoncurrentLifecyclePage.getMockImplementation()!
    harness.database.evaluateNoncurrentLifecyclePage.mockImplementationOnce(async (...args) => {
      const page = await evaluate(...args)
      clock += 1000
      return page
    })

    await expect(harness.run()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(harness.database.armLifecycleAttempt).toHaveBeenCalledOnce()
    expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledOnce()
  })

  it('stops starting new batches once the signal aborts and releases as PARTIAL', async () => {
    const controller = new AbortController()
    const harness = createHarness({})
    const page = {
      rawRowsExamined: 1,
      candidates: [harness.pageCandidate],
      pageEnd: { name: harness.pageCandidate.name, archivedAt: harness.pageCandidate.archivedAt },
      exhausted: false,
    }
    harness.database.evaluateNoncurrentLifecyclePage
      .mockResolvedValueOnce(page)
      .mockResolvedValue(emptyPage)
    harness.database.resolveLifecycleAbandonedArtifacts.mockImplementationOnce(() => {
      controller.abort()
    })

    await expect(
      harness.executor.run(bucketId, {
        tenantLifecycleEnabled: true,
        signal: controller.signal,
      })
    ).resolves.toMatchObject({ status: 'PARTIAL' })
    expect(harness.database.armLifecycleAttempt).toHaveBeenCalledOnce()
    expect(harness.database.evaluateNoncurrentLifecyclePage).toHaveBeenCalledOnce()
    expect(harness.database.releaseLifecycleClaim).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ nextRunAt: new Date(now.getTime() + 1000).toISOString() })
    )
  })

  it.each([
    { deleteOutcome: 'FAILED', failureCount: 0, delayMinutes: 5 },
    { deleteOutcome: 'UNKNOWN', failureCount: 6, delayMinutes: 15 },
  ] as const)(
    'stops after a $deleteOutcome backend key without arming another batch and backs off',
    async ({ deleteOutcome, failureCount, delayMinutes }) => {
      const harness = createHarness({ deleteOutcome, failureCount })
      harness.database.evaluateNoncurrentLifecyclePage.mockResolvedValueOnce(fullPage(49))

      await expect(harness.run()).resolves.toMatchObject({ status: 'PARTIAL' })
      expect(harness.database.armLifecycleAttempt).toHaveBeenCalledOnce()
      expect(harness.database.completeLifecycleRun).not.toHaveBeenCalled()
      const [deleted, failed] = harness.database.resolveLifecycleAbandonedArtifacts.mock.calls[0]
      expect(deleted).toEqual([])
      expect(failed).toHaveLength(96)
      const released = harness.database.releaseLifecycleClaim.mock.calls[0][0]
      expect(released).toMatchObject({
        nextRunAt: new Date(now.getTime() + delayMinutes * 60_000).toISOString(),
        error: {
          name: 'LifecycleDeletionIncomplete',
          message: deleteOutcome.toLowerCase(),
          pendingArtifacts: 96,
        },
      })
      expect(released).not.toHaveProperty('continuation')
    }
  )

  it('compiles rules from the persisted snapshot instead of the current time', async () => {
    const continuation = newContinuation(randomUUID(), { snapshotAt: '2026-08-12T00:00:00.000Z' })
    const harness = createHarness({ continuation })

    await harness.run()

    expect(harness.database.evaluateNoncurrentLifecyclePage.mock.calls[0][0]).toMatchObject({
      bucketId,
      snapshotAt: '2026-08-12T00:00:00.000Z',
      pageSize: 480,
      rules: [{ cutoffAt: '2026-08-11T00:00:00.000Z' }],
    })
  })

  it.each([
    { failureCount: 6, delayMinutes: 320 },
    { failureCount: 10, delayMinutes: 360 },
  ])(
    'releases without rereading the continuation after $failureCount failures when the send throws',
    async ({ failureCount, delayMinutes }) => {
      const harness = createHarness({ failureCount })
      const failure = new Error('connection reset')
      harness.backend.deleteObjectsDetailed.mockRejectedValueOnce(failure)

      await expect(harness.run()).rejects.toBe(failure)
      expect(harness.database.revalidateLifecycleClaim).not.toHaveBeenCalled()
      const release = harness.database.releaseLifecycleClaim.mock.calls.at(-1)![0]
      expect(release).toMatchObject({
        nextRunAt: new Date(now.getTime() + delayMinutes * 60_000).toISOString(),
        error: expect.objectContaining({ message: failure.message }),
      })
      expect(release).not.toHaveProperty('continuation')
      expect(harness.database.resolveLifecycleAbandonedArtifacts).not.toHaveBeenCalled()
    }
  )

  it.each([null, '856d94e5-5241-42ae-9c0b-d06332a7ae15'])(
    'deletes exactly the physical version %s for an unversioned data row',
    async (version) => {
      const harness = createHarness({})
      harness.pageCandidate.version = version
      const name = harness.pageCandidate.name
      const keys = version === null ? [name] : [`${name}/${version}`, `${name}/${version}.info`]

      await expect(harness.run()).resolves.toMatchObject({ status: 'COMPLETED' })
      expect(harness.backend.deleteObjectsDetailed).toHaveBeenCalledWith(
        'root',
        keys.map((key) => `tenant/bucket/${key}`),
        { signal: expect.any(AbortSignal) }
      )
      expect(harness.calls).toEqual(['arm', 'delete', 'resolve', 'complete'])
    }
  )

  it('reports a non-empty error for an empty backend error', async () => {
    const harness = createHarness({})
    harness.backend.deleteObjectsDetailed.mockImplementationOnce((_bucket, keys: string[]) =>
      keys.map((key) => ({ key, outcome: 'UNKNOWN', error: { message: '' } }))
    )
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})

    await expect(harness.run()).resolves.toMatchObject({
      status: 'PARTIAL',
      runId: expect.any(String),
    })
    expect(harness.database.releaseLifecycleClaim.mock.calls[0][0].error.message).toBe(
      'Lifecycle deletion remains incomplete'
    )
    expect(JSON.parse(warning.mock.calls[0][2].metadata as string)).not.toHaveProperty('error')
  })

  it('counts only the failed artifacts as pending after a partial backend deletion', async () => {
    const harness = createHarness({})
    harness.backend.deleteObjectsDetailed.mockImplementationOnce((_bucket, keys: string[]) =>
      keys.map((key, index) => ({
        key,
        outcome: index === 0 ? 'DELETED' : 'FAILED',
        error: { message: 'nope' },
      }))
    )
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})

    await expect(harness.run()).resolves.toMatchObject({ status: 'PARTIAL' })
    expect(harness.database.releaseLifecycleClaim.mock.calls[0][0].error.pendingArtifacts).toBe(1)
    expect(JSON.parse(warning.mock.calls[0][2].metadata as string).failedArtifacts).toBe(1)
  })
})
