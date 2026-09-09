import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import { logSchema } from '@internal/monitoring'
import { isMissingBackendObject } from '@storage/backend'
import {
  armLifecycleAttempt,
  createLifecycleContinuation,
  freezeLifecycleBatchVersion,
  stageLifecycleBatch,
} from '@storage/lifecycle/continuation'
import { NoncurrentLifecycleExecutor } from '@storage/lifecycle/executor'
import { getConfig } from '../config'
import { useLifecycleVersioningFixtures } from './utils/lifecycle-versioning'
import { useStorage, withDeleteEnabled } from './utils/storage'

const { storageBackendType, storageS3Bucket, tenantId } = getConfig()
const DAY_MS = 24 * 60 * 60 * 1000

describe('noncurrent lifecycle executor against explicit history fixtures', () => {
  const helper = useStorage()
  const fixtures = useLifecycleVersioningFixtures()
  const uploaded: Array<{ name: string; version: string | null }> = []
  let bucketId: string
  const configuration = {
    rules: [
      {
        id: 'expire-history',
        status: 'Enabled' as const,
        filter: {},
        noncurrentVersionExpiration: { noncurrentDays: 1 },
      },
    ],
  }

  beforeEach(async () => {
    bucketId = fixtures.trackBucket(`lifecycle-executor-${randomUUID()}`)
    await helper.database.createBucket({ id: bucketId, name: bucketId })
  })

  afterEach(async () => {
    const cleanup = await Promise.allSettled(
      uploaded
        .splice(0)
        .map(({ name, version }) =>
          helper.adapter.deleteObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, version)
        )
    )
    await withDeleteEnabled(helper.database.connection, async (transaction) => {
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.bucket_lifecycle_states WHERE bucket_id = $1', [
        bucketId,
      ])
      await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
    })
    expect(cleanup.filter((result) => result.status === 'rejected')).toEqual([])
  })

  async function uploadFixture(name: string, version: string | null) {
    uploaded.push({ name, version })
    await helper.adapter.uploadObject(
      storageS3Bucket,
      `${tenantId}/${bucketId}/${name}`,
      version,
      Readable.from(Buffer.from('x')),
      'text/plain',
      'no-cache',
      undefined,
      1
    )
  }

  async function seedHistory(
    name: string,
    archivedVersion: string | null,
    currentVersion: string,
    options: { isVersioned?: boolean; archivedAt?: Date } = {}
  ) {
    await uploadFixture(name, archivedVersion)
    // The file layout cannot retain an unsuffixed blob beside a revision directory.
    // In that case only the current metadata is seeded; MinIO covers both blobs.
    if (archivedVersion !== null || storageBackendType === 's3') {
      await uploadFixture(name, currentVersion)
    }
    await helper.database.connection.query(
      `INSERT INTO storage.objects
         (id, bucket_id, name, version, is_versioned, metadata, created_at, archived_at)
       VALUES
         (gen_random_uuid(), $1, $2, $3, $5, '{"size":1,"mimetype":"text/plain"}',
          '2000-01-01', $6),
         (gen_random_uuid(), $1, $2, $4, true, '{"size":1,"mimetype":"text/plain"}',
          clock_timestamp(), NULL)`,
      [
        bucketId,
        name,
        archivedVersion,
        currentVersion,
        options.isVersioned ?? true,
        options.archivedAt ?? new Date(),
      ]
    )
  }

  async function activate(status: 'ENABLED' | 'SUSPENDED' = 'ENABLED') {
    await fixtures.setStatus(bucketId, status)
    await helper.database.putLifecycleConfiguration(bucketId, configuration)
  }

  function runExecutor(snapshotAt?: Date) {
    return new NoncurrentLifecycleExecutor(helper.storage, {
      deleteEnabled: true,
      now: () => snapshotAt ?? new Date(Date.now() + 3 * DAY_MS),
    }).run(
      { bucketId, scanKind: 'NONCURRENT', shardEpoch: '1', shardId: 0 },
      { tenantVersioningEnabled: true }
    )
  }

  async function versions(name: string) {
    const result = await helper.database.connection.query<{
      version: string | null
      current: boolean
    }>(
      `SELECT version, archived_at IS NULL AS current
       FROM storage.objects WHERE bucket_id = $1 AND name = $2 ORDER BY created_at`,
      [bucketId, name]
    )
    return result.rows
  }

  async function expectBlobAbsent(name: string, version: string | null) {
    const missing = await helper.adapter
      .headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, version, {
        confirmMissing: true,
      })
      .then(
        () => false,
        (error: unknown) => isMissingBackendObject(error)
      )
    expect(missing).toBe(true)
  }

  it('preserves malformed continuation JSON and releases its committed claim after decoding fails', async () => {
    await activate()
    const continuation = { continuationVersion: 999 }
    await helper.database.connection.query(
      'UPDATE storage.bucket_lifecycle_states SET continuation = $2::jsonb WHERE bucket_id = $1',
      [bucketId, JSON.stringify(continuation)]
    )

    await expect(runExecutor()).rejects.toThrow()
    const result = await helper.database.connection.query(
      'SELECT claim_id, claim_until, failure_count, last_error, continuation FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
      [bucketId]
    )
    expect(result.rows).toEqual([
      expect.objectContaining({
        claim_id: null,
        claim_until: null,
        failure_count: 1,
        continuation,
        last_error: expect.objectContaining({ name: 'LifecycleContinuationDecodeError' }),
      }),
    ])
  })

  describe.each([
    { gate: 'fleet', deleteEnabled: false, tenantVersioningEnabled: true },
    { gate: 'tenant', deleteEnabled: true, tenantVersioningEnabled: false },
  ])('with the $gate deletion gate closed', ({ deleteEnabled, tenantVersioningEnabled }) => {
    it('leaves fresh history and its continuation untouched', async () => {
      await activate()
      const name = 'history/paused.txt'
      const archived = randomUUID()
      const current = randomUUID()
      await seedHistory(name, archived, current)
      const before = await versions(name)

      await expect(
        new NoncurrentLifecycleExecutor(helper.storage, { deleteEnabled }).run(
          { bucketId, scanKind: 'NONCURRENT', shardEpoch: '1', shardId: 0 },
          { tenantVersioningEnabled }
        )
      ).resolves.toMatchObject({ status: 'PAUSED' })

      expect(await versions(name)).toEqual(before)
      for (const version of [archived, current]) {
        await expect(
          helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, version)
        ).resolves.toMatchObject({ size: 1 })
      }
      const state = await helper.database.connection.query(
        'SELECT continuation, claim_id, next_run_at FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
        [bucketId]
      )
      expect(state.rows).toEqual([
        { continuation: null, claim_id: null, next_run_at: expect.any(Date) },
      ])
    })
  })

  describe.each([
    { gate: 'fleet gate closed', deleteEnabled: false, tenantVersioningEnabled: true },
    { gate: 'tenant gate closed', deleteEnabled: true, tenantVersioningEnabled: false },
    { gate: 'both gates open', deleteEnabled: true, tenantVersioningEnabled: true },
  ])('armed recovery with $gate', ({ deleteEnabled, tenantVersioningEnabled }) => {
    const canRedrive = deleteEnabled && tenantVersioningEnabled
    it.each([true, false])('recovers an armed journal with artifacts absent=%s', async (absent) => {
      await activate()
      const name = 'history/recovery.txt'
      const archived = randomUUID()
      const current = randomUUID()
      const archivedAt = new Date(Date.now() - 3 * DAY_MS)
      await seedHistory(name, archived, current, { archivedAt })
      await uploadFixture(name, `${archived}.info`)
      const bucket = await helper.database.findLifecycleBucket(bucketId)
      const generation = bucket.lifecycle_configuration_generation!
      const identity = {
        bucketId,
        scanKind: 'NONCURRENT' as const,
        shardEpoch: '1',
        shardId: 0,
        claimId: randomUUID(),
      }
      await expect(
        helper.database.claimLifecycleShard({ ...identity, leaseMs: 60_000 })
      ).resolves.toBeDefined()
      const staged = stageLifecycleBatch(
        createLifecycleContinuation({
          runId: randomUUID(),
          trigger: 'scheduled',
          generation,
          snapshotAt: new Date().toISOString(),
          topology: { scanKind: 'NONCURRENT', epoch: '1', shardId: 0, shardCount: 1 },
        }),
        {
          rawRowsExamined: 1,
          pageEnd: { name, archivedAt: archivedAt.toISOString() },
          versions: [
            freezeLifecycleBatchVersion({ name, version: archived, isDeleteMarker: false }),
          ],
        }
      )
      await helper.database.saveLifecycleContinuation(identity, staged)
      const armed = armLifecycleAttempt(staged, {
        attemptId: randomUUID(),
        authorizedGeneration: generation,
        startedAt: new Date(Date.now() - DAY_MS).toISOString(),
      })
      await expect(
        helper.database.armLifecycleAttempt({
          ...identity,
          configurationGeneration: generation,
          leaseMs: 60_000,
          continuation: armed,
        })
      ).resolves.toMatchObject({ continuation: armed })
      // Simulate a worker dying after its durable arm, optionally after deleting its bytes.
      if (absent) {
        await helper.adapter.deleteObject(
          storageS3Bucket,
          `${tenantId}/${bucketId}/${name}`,
          archived
        )
        await helper.adapter.deleteObject(
          storageS3Bucket,
          `${tenantId}/${bucketId}/${name}`,
          `${archived}.info`
        )
      }
      await helper.database.releaseLifecycleShardClaim({
        ...identity,
        nextRunAt: '2000-01-01T00:00:00.000Z',
      })

      const heads = vi.spyOn(helper.adapter, 'headObject')
      const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')
      try {
        await expect(
          new NoncurrentLifecycleExecutor(helper.storage, { deleteEnabled }).run(identity, {
            tenantVersioningEnabled,
          })
        ).resolves.toMatchObject({
          status: canRedrive ? 'COMPLETED' : 'PAUSED',
          runId: armed.runId,
        })
        expect(heads).toHaveBeenCalledTimes(canRedrive ? 0 : 2)
        expect(deletes).toHaveBeenCalledTimes(canRedrive ? 1 : 0)
        if (canRedrive) {
          expect(deletes).toHaveBeenCalledWith(storageS3Bucket, [
            `${tenantId}/${bucketId}/${name}/${archived}`,
            `${tenantId}/${bucketId}/${name}/${archived}.info`,
          ])
        }
      } finally {
        heads.mockRestore()
        deletes.mockRestore()
      }

      const state = await helper.database.connection.query(
        'SELECT continuation, claim_id, next_run_at, last_result FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
        [bucketId]
      )
      expect(state.rows[0]).toMatchObject({ claim_id: null, next_run_at: expect.any(Date) })
      if (canRedrive) {
        expect(state.rows[0]).toMatchObject({
          continuation: null,
          last_result: {
            runId: armed.runId,
            counters: { objectVersionsDeleted: 1, bytesDeleted: 1, batchesCompleted: 1 },
          },
        })
      }
      if (absent || canRedrive) {
        expect(await versions(name)).toEqual([{ version: current, current: true }])
        if (!canRedrive) {
          expect(state.rows[0].continuation.batch).toBeUndefined()
          expect(state.rows[0].continuation.counters.objectVersionsDeleted).toBe(1)
        }
        await expectBlobAbsent(name, archived)
      } else {
        expect(await versions(name)).toContainEqual({ version: archived, current: false })
        expect(state.rows[0].continuation).toEqual(armed)
        await expect(
          helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, archived)
        ).resolves.toMatchObject({ size: 1 })
      }
      await expect(
        helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, current)
      ).resolves.toMatchObject({ size: 1 })
    })
  })

  it.each([
    'ENABLED',
    'SUSPENDED',
  ] as const)('expires archived fixture bytes while %s and retains the current revision', async (status) => {
    await activate(status)
    const name = 'history/versioned.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current)

    await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    await expectBlobAbsent(name, archived)
    await expect(
      helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, current)
    ).resolves.toMatchObject({ size: 1 })
  })

  it('expires a legacy physical null revision and its unsuffixed blob', async () => {
    await activate()
    const name = 'history/legacy-null.txt'
    const current = randomUUID()
    await seedHistory(name, null, current, { isVersioned: false })
    expect(await versions(name)).toContainEqual({ version: null, current: false })

    await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    await expectBlobAbsent(name, null)
    if (storageBackendType === 's3') {
      await expect(
        helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, current)
      ).resolves.toMatchObject({ size: 1 })
    }
  })

  it('expires an unversioned logical null revision stored under a physical UUID', async () => {
    await activate()
    const name = 'history/unversioned.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current, { isVersioned: false })

    await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    await expectBlobAbsent(name, archived)
  })

  it('expires two-day-old history at the real clock without a physical write stamp', async () => {
    await activate()
    const name = 'history/two-days-old.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current, { archivedAt: new Date(Date.now() - 2 * DAY_MS) })

    await expect(runExecutor(new Date())).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    await expectBlobAbsent(name, archived)
    await expect(
      helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, current)
    ).resolves.toMatchObject({ size: 1 })
  })

  it.each([
    { label: 'null metadata', metadata: null, bytes: 0 },
    { label: 'missing size', metadata: {}, bytes: 0 },
    { label: 'null size', metadata: { size: null }, bytes: 0 },
    { label: 'negative size', metadata: { size: -1 }, bytes: 0 },
    { label: 'fractional size', metadata: { size: 1.5 }, bytes: 0 },
    { label: 'nonnumeric size', metadata: { size: 'invalid' }, bytes: 0 },
    { label: 'empty size', metadata: { size: '' }, bytes: 0 },
    { label: 'boolean size', metadata: { size: true }, bytes: 0 },
    { label: 'unsafe size', metadata: { size: Number.MAX_SAFE_INTEGER + 1 }, bytes: 0 },
    { label: 'numeric string size', metadata: { size: '1' }, bytes: 1 },
  ])('continues past $label and preserves byte accounting', async ({ metadata, bytes }) => {
    await activate()
    const archivedAt = new Date(Date.now() - 3 * DAY_MS)
    const first = { name: 'history/a.txt', archived: randomUUID(), current: randomUUID() }
    const second = { name: 'history/b.txt', archived: randomUUID(), current: randomUUID() }
    for (const row of [first, second]) {
      await seedHistory(row.name, row.archived, row.current, { archivedAt })
    }
    await helper.database.connection.query(
      'UPDATE storage.objects SET metadata = $4::jsonb WHERE bucket_id = $1 AND name = $2 AND version = $3',
      [bucketId, first.name, first.archived, metadata === null ? null : JSON.stringify(metadata)]
    )

    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => undefined)
    try {
      await expect(
        new NoncurrentLifecycleExecutor(helper.storage, { deleteEnabled: true, pageSize: 1 }).run(
          { bucketId, scanKind: 'NONCURRENT', shardEpoch: '1', shardId: 0 },
          { tenantVersioningEnabled: true }
        )
      ).resolves.toMatchObject({ status: 'COMPLETED' })
      if (bytes === 0) {
        expect(warning).toHaveBeenCalledExactlyOnceWith(
          expect.anything(),
          '[Lifecycle] Missing or invalid object sizes counted as zero bytes',
          expect.objectContaining({
            tenantId,
            metadata: JSON.stringify({ bucketId, invalidSizeCount: 1 }),
          })
        )
      } else {
        expect(warning).not.toHaveBeenCalled()
      }
    } finally {
      warning.mockRestore()
    }

    const result = await helper.database.connection.query(
      'SELECT continuation, last_result FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
      [bucketId]
    )
    expect(result.rows[0]).toMatchObject({
      continuation: null,
      last_result: { counters: { objectVersionsDeleted: 2, bytesDeleted: bytes + 1 } },
    })
    for (const row of [first, second]) {
      expect(await versions(row.name)).toEqual([{ version: row.current, current: true }])
      await expectBlobAbsent(row.name, row.archived)
      await expect(
        helper.adapter.headObject(
          storageS3Bucket,
          `${tenantId}/${bucketId}/${row.name}`,
          row.current
        )
      ).resolves.toMatchObject({ size: 1 })
    }
  })

  it('persists incomplete-deletion backoff and clears the failure after recovery', async () => {
    await activate()
    const name = 'history/retry.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current, { archivedAt: new Date(Date.now() - 3 * DAY_MS) })
    await uploadFixture(name, `${archived}.info`)
    const dataKey = `${tenantId}/${bucketId}/${name}/${archived}`
    const infoKey = `${dataKey}.info`
    let failInfo = true
    let now = new Date()
    const deleteObjects = helper.adapter.deleteObjectsDetailed.bind(helper.adapter)
    const deletes = vi
      .spyOn(helper.adapter, 'deleteObjectsDetailed')
      .mockImplementation(async (bucket, keys) => {
        if (!failInfo) return deleteObjects(bucket, keys)
        const allowed = keys.filter((key) => key !== infoKey)
        const results = allowed.length > 0 ? await deleteObjects(bucket, allowed) : []
        return [
          ...results,
          ...keys
            .filter((key) => key === infoKey)
            .map((key) => ({
              key,
              outcome: 'FAILED' as const,
              error: { code: 'AccessDenied', message: 'info deletion denied' },
            })),
        ]
      })

    const run = () =>
      new NoncurrentLifecycleExecutor(helper.storage, { deleteEnabled: true, now: () => now }).run(
        { bucketId, scanKind: 'NONCURRENT', shardEpoch: '1', shardId: 0 },
        { tenantVersioningEnabled: true }
      )
    try {
      for (const [index, delayMinutes] of [5, 10].entries()) {
        await expect(run()).resolves.toMatchObject({ status: 'PARTIAL' })
        const nextRunAt = new Date(now.getTime() + delayMinutes * 60_000)
        const state = await helper.database.connection.query(
          'SELECT continuation, claim_id, failure_count, last_error, next_run_at FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
          [bucketId]
        )
        expect(state.rows[0]).toMatchObject({
          claim_id: null,
          failure_count: index + 1,
          last_error: {
            name: 'LifecycleDeletionIncomplete',
            message: 'info deletion denied',
            pendingArtifacts: 1,
          },
          next_run_at: nextRunAt,
          continuation: { counters: { objectVersionsDeleted: 0, bytesDeleted: 0 } },
        })
        expect(state.rows[0].continuation.batch.versions[0].artifacts).toEqual([
          expect.objectContaining({ outcome: 'DELETED' }),
          expect.objectContaining({ outcome: 'FAILED' }),
        ])
        expect(await versions(name)).toContainEqual({ version: archived, current: false })
        await expectBlobAbsent(name, archived)
        await expect(
          helper.adapter.headObject(
            storageS3Bucket,
            `${tenantId}/${bucketId}/${name}`,
            `${archived}.info`
          )
        ).resolves.toMatchObject({ size: 1 })

        // Advance this fixture's due time without waiting for wall-clock backoff.
        await helper.database.connection.query(
          "UPDATE storage.bucket_lifecycle_states SET next_run_at = '2000-01-01' WHERE bucket_id = $1",
          [bucketId]
        )
        now = nextRunAt
      }
      failInfo = false
      await expect(run()).resolves.toMatchObject({ status: 'COMPLETED' })
      expect(deletes.mock.calls.map(([, keys]) => keys)).toEqual([
        [dataKey, infoKey],
        [infoKey],
        [infoKey],
      ])
    } finally {
      deletes.mockRestore()
    }

    const result = await helper.database.connection.query(
      'SELECT continuation, failure_count, last_error, last_result FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
      [bucketId]
    )
    expect(result.rows[0]).toMatchObject({
      continuation: null,
      failure_count: 0,
      last_error: null,
      last_result: { counters: { objectVersionsDeleted: 1, bytesDeleted: 1 } },
    })
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    await expectBlobAbsent(name, `${archived}.info`)
    await expect(
      helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, current)
    ).resolves.toMatchObject({ size: 1 })
  })
})
