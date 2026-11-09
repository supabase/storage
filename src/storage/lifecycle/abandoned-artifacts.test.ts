import { logger, logSchema } from '@internal/monitoring'
import * as config from '../../config'
import type { Database } from '../database'
import type { Storage } from '../storage'
import { retryAbandonedLifecycleArtifacts } from './abandoned-artifacts'

const attemptedBefore = '2026-08-15T00:00:00.000Z'

function createHarness(outcomesByCall: Array<Array<'DELETED' | 'FAILED' | 'UNKNOWN'>> = []) {
  let call = 0
  const database = {
    tenantId: 'tenant',
    listLifecycleAbandonedArtifacts: vi.fn().mockResolvedValue([]),
    resolveLifecycleAbandonedArtifacts: vi.fn().mockResolvedValue(undefined),
  }
  const backend = {
    deleteObjectsDetailed: vi
      .fn()
      .mockImplementation((_bucket: string, keys: string[], _options?: { signal: AbortSignal }) => {
        const outcomes = outcomesByCall[call++] ?? []
        return keys.map((key, index) => ({ key, outcome: outcomes[index] ?? 'DELETED' }))
      }),
  }
  const storage = {
    backend,
    location: {
      getRootLocation: () => 'root',
      getKeyLocation: ({ tenantId, bucketId, objectName }: Record<string, string>) =>
        `${tenantId}/${bucketId}/${objectName}`,
    },
  } as unknown as Storage
  const retry = (signal?: AbortSignal, budgetMs = 60_000) =>
    retryAbandonedLifecycleArtifacts({
      storage,
      database: database as unknown as Database,
      attemptedBefore,
      budgetMs,
      signal,
    })
  return { backend, database, retry }
}

describe('retryAbandonedLifecycleArtifacts', () => {
  it('deletes per-bucket keys, resolves outcomes and stops when a page deletes nothing', async () => {
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    const harness = createHarness([['DELETED', 'FAILED'], ['FAILED']])
    harness.database.listLifecycleAbandonedArtifacts
      .mockResolvedValueOnce([
        { bucketId: 'one', key: 'a/v1', live: false },
        { bucketId: 'two', key: 'b/v2.info', live: false },
      ])
      .mockResolvedValueOnce([{ bucketId: 'two', key: 'b/v2.info', live: false }])

    await harness.retry()

    expect(harness.database.listLifecycleAbandonedArtifacts).toHaveBeenCalledTimes(2)
    expect(harness.database.listLifecycleAbandonedArtifacts).toHaveBeenCalledWith(
      attemptedBefore,
      1000
    )
    expect(harness.backend.deleteObjectsDetailed.mock.calls.map(([, keys]) => keys)).toEqual([
      ['tenant/one/a/v1', 'tenant/two/b/v2.info'],
      ['tenant/two/b/v2.info'],
    ])
    expect(harness.database.resolveLifecycleAbandonedArtifacts.mock.calls).toEqual([
      [
        [{ bucketId: 'one', key: 'a/v1', live: false }],
        [{ bucketId: 'two', key: 'b/v2.info', live: false }],
      ],
      [[], [{ bucketId: 'two', key: 'b/v2.info', live: false }]],
    ])
    expect(warning).toHaveBeenCalledTimes(2)
    expect(warning).toHaveBeenCalledWith(
      logger,
      '[Lifecycle] Abandoned artifacts remain after retry',
      {
        type: 'event',
        tenantId: 'tenant',
        project: 'tenant',
        metadata: JSON.stringify({ failed: 1 }),
      }
    )
  })

  it('stops after the budget even when a page was deleted', async () => {
    const clock = vi.spyOn(Date, 'now').mockReturnValue(1000)
    const harness = createHarness()
    harness.database.listLifecycleAbandonedArtifacts.mockResolvedValueOnce([
      { bucketId: 'one', key: 'a/v1', live: false },
    ])
    harness.backend.deleteObjectsDetailed.mockImplementationOnce(
      async (_bucket, keys: string[]) => {
        clock.mockReturnValue(2000)
        return keys.map((key) => ({ key, outcome: 'DELETED' }))
      }
    )

    await harness.retry(undefined, 1000)

    expect(harness.database.listLifecycleAbandonedArtifacts).toHaveBeenCalledOnce()
    expect(harness.database.resolveLifecycleAbandonedArtifacts).toHaveBeenCalledOnce()
  })

  it('awaits the resolution of each page before reporting', async () => {
    const harness = createHarness()
    harness.database.listLifecycleAbandonedArtifacts.mockResolvedValueOnce([
      { bucketId: 'one', key: 'a/v1', live: false },
    ])
    const failure = new Error('resolve failed')
    harness.database.resolveLifecycleAbandonedArtifacts.mockRejectedValueOnce(failure)

    await expect(harness.retry()).rejects.toBe(failure)
  })

  it('bounds each deletion by the remaining budget with a minimum send time', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    try {
      const timeout = vi.spyOn(AbortSignal, 'timeout')
      const harness = createHarness()
      harness.database.listLifecycleAbandonedArtifacts
        .mockResolvedValueOnce([{ bucketId: 'one', key: 'a/v1', live: false }])
        .mockResolvedValueOnce([{ bucketId: 'one', key: 'a/v2', live: false }])
        .mockResolvedValueOnce([{ bucketId: 'one', key: 'a/v3', live: false }])
      for (const advance of [20_000, 35_000]) {
        harness.backend.deleteObjectsDetailed.mockImplementationOnce(
          async (_bucket, keys: string[]) => {
            vi.advanceTimersByTime(advance)
            return keys.map((key) => ({ key, outcome: 'DELETED' }))
          }
        )
      }

      await harness.retry()

      expect(timeout.mock.calls.map(([ms]) => ms)).toEqual([60_000, 40_000, 10_000])
    } finally {
      vi.useRealTimers()
    }
  })

  it('never sends live entries to the backend and warns with the dropped count', async () => {
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    const harness = createHarness()
    harness.database.listLifecycleAbandonedArtifacts.mockResolvedValueOnce([
      { bucketId: 'one', key: 'a/v1', live: true },
      { bucketId: 'one', key: 'a/v1.info', live: true },
      { bucketId: 'two', key: 'b/v2', live: false },
    ])

    await harness.retry()

    expect(harness.backend.deleteObjectsDetailed.mock.calls.map(([, keys]) => keys)).toEqual([
      ['tenant/two/b/v2'],
    ])
    expect(harness.database.resolveLifecycleAbandonedArtifacts).toHaveBeenCalledExactlyOnceWith(
      [{ bucketId: 'two', key: 'b/v2', live: false }],
      []
    )
    expect(warning).toHaveBeenCalledExactlyOnceWith(
      logger,
      '[Lifecycle] Dropped abandoned artifacts owned by live objects',
      {
        type: 'event',
        tenantId: 'tenant',
        project: 'tenant',
        metadata: JSON.stringify({ dropped: 2 }),
      }
    )
  })

  it('continues to the next page when a page holds only live entries', async () => {
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    const harness = createHarness()
    harness.database.listLifecycleAbandonedArtifacts
      .mockResolvedValueOnce([{ bucketId: 'one', key: 'a/v1', live: true }])
      .mockResolvedValueOnce([{ bucketId: 'one', key: 'b/v2', live: false }])

    await harness.retry()

    expect(harness.database.listLifecycleAbandonedArtifacts).toHaveBeenCalledTimes(3)
    expect(harness.backend.deleteObjectsDetailed.mock.calls.map(([, keys]) => keys)).toEqual([
      ['tenant/one/b/v2'],
    ])
    expect(warning).toHaveBeenCalledOnce()
  })

  it('keeps paging after a page that only held keys beyond the S3 limit', async () => {
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      storageBackendType: 's3',
    })
    const harness = createHarness()
    harness.database.listLifecycleAbandonedArtifacts
      .mockResolvedValueOnce([{ bucketId: 'one', key: 'a'.repeat(1100), live: false }])
      .mockResolvedValueOnce([{ bucketId: 'one', key: 'b/v2', live: false }])

    await harness.retry()

    expect(harness.database.listLifecycleAbandonedArtifacts).toHaveBeenCalledTimes(3)
    expect(harness.backend.deleteObjectsDetailed.mock.calls.map(([, keys]) => keys)).toEqual([
      ['tenant/one/b/v2'],
    ])
  })

  it('returns immediately when nothing is abandoned', async () => {
    const harness = createHarness()

    await harness.retry()

    expect(harness.backend.deleteObjectsDetailed).not.toHaveBeenCalled()
    expect(harness.database.resolveLifecycleAbandonedArtifacts).not.toHaveBeenCalled()
  })

  it('does nothing once aborted', async () => {
    const harness = createHarness()

    await harness.retry(AbortSignal.abort())

    expect(harness.database.listLifecycleAbandonedArtifacts).not.toHaveBeenCalled()
  })
})
