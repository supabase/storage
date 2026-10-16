import { randomUUID } from 'node:crypto'
import { compileLifecycleEvaluationRules } from '@storage/lifecycle/configuration'
import {
  armLifecycleAttempt,
  freezeLifecycleBatchVersion,
  recordLifecycleArtifactOutcomes,
  stageLifecycleBatch,
} from '@storage/lifecycle/continuation'
import type { NoncurrentLifecycleShardCursor } from '@storage/schemas'
import { Client } from 'pg'
import { getConfig } from '../config'
import { armedContinuation, newContinuation } from './utils/lifecycle-continuation'
import { shard, useLifecycleStorage } from './utils/lifecycle-versioning'

describe('noncurrent lifecycle database API', () => {
  const fixtures = useLifecycleStorage('lifecycle-db')
  const tHelper = fixtures.helper
  const { withServiceOperation } = fixtures
  let bucketId: string
  let analyticsBucketId: string

  beforeEach(async () => {
    bucketId = fixtures.bucketId
    analyticsBucketId = await fixtures.createBucket('ANALYTICS')
  })

  async function insertArchivedObjects(
    rows: Array<{ id?: string; name: string; archivedAt: string; version?: string }>
  ) {
    await tHelper.database.connection.query(
      `INSERT INTO storage.objects (id, bucket_id, name, version, archived_at, is_versioned, metadata)
       SELECT COALESCE(r.id::uuid, gen_random_uuid()), $1, r.name, COALESCE(r.version, gen_random_uuid()::text),
              r.archived_at::timestamptz, true, '{"size": 10}'::jsonb
       FROM unnest($2::text[], $3::text[], $4::text[], $5::text[]) AS r(name, archived_at, version, id)`,
      [
        bucketId,
        rows.map((row) => row.name),
        rows.map((row) => row.archivedAt),
        rows.map((row) => row.version ?? null),
        rows.map((row) => row.id ?? null),
      ]
    )
  }

  async function putConfiguration(newerNoncurrentVersions?: number) {
    await fixtures.setStatus(bucketId, 'ENABLED')
    const bucket = await tHelper.database.putLifecycleConfiguration(bucketId, {
      rules: [
        {
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: {
            noncurrentDays: 30,
            ...(newerNoncurrentVersions === undefined ? {} : { newerNoncurrentVersions }),
          },
        },
      ],
    })
    return bucket.lifecycle_configuration_generation!
  }

  test('loads typed Standard-bucket authority', async () => {
    const generation = await putConfiguration()
    const bucket = await tHelper.database.findLifecycleBucket(bucketId)

    expect(bucket).toMatchObject({
      id: bucketId,
      type: 'STANDARD',
      lifecycle_configuration_generation: generation,
      lifecycle_configuration: {
        rules: [
          {
            status: 'Enabled',
            filter: {},
            noncurrentVersionExpiration: { noncurrentDays: 30 },
          },
        ],
      },
    })
    await expect(tHelper.database.findLifecycleBucket(analyticsBucketId)).rejects.toMatchObject({
      code: 'InvalidRequest',
    })
    await expect(
      tHelper.database.putLifecycleConfiguration(analyticsBucketId, bucket.lifecycle_configuration!)
    ).rejects.toMatchObject({ code: 'InvalidRequest', httpStatusCode: 400 })
  })

  test('lists, claims, and revalidates only Standard-bucket shard state', async () => {
    await putConfiguration()

    await tHelper.database.connection.query(
      `INSERT INTO storage.bucket_lifecycle_states (
         bucket_id, scan_kind, shard_id, shard_epoch, shard_count, next_run_at
       ) VALUES ($1, 'NONCURRENT', 0, 1, 1, now())`,
      [analyticsBucketId]
    )

    const due = await tHelper.database.listDueLifecycleShards(100)
    expect(due).toContainEqual(shard(bucketId))
    expect(due.some((coordinate) => coordinate.bucketId === analyticsBucketId)).toBe(false)

    const claimId = randomUUID()
    await expect(
      tHelper.database.claimLifecycleShard({
        ...shard(analyticsBucketId),
        claimId: randomUUID(),
        leaseMs: 60_000,
      })
    ).resolves.toBeUndefined()

    await expect(
      tHelper.database.claimLifecycleShard({ ...shard(bucketId), claimId, leaseMs: 60_000 })
    ).resolves.toMatchObject({ claimId })

    await expect(
      tHelper.database.revalidateLifecycleShardClaim({ ...shard(bucketId), claimId })
    ).resolves.toMatchObject({ claimId })

    await expect(
      tHelper.database.revalidateLifecycleShardClaim({ ...shard(bucketId), claimId: randomUUID() })
    ).resolves.toBeUndefined()
  })

  test.each([
    Number.MAX_SAFE_INTEGER,
    800_000,
    2_460_000,
  ])('evaluates a policy with %i days without timestamp overflow', async (noncurrentDays) => {
    await insertArchivedObjects([{ name: 'overflow', archivedAt: '2026-01-01T00:00:00.000Z' }])
    const snapshotAt = new Date('2026-08-15T00:00:00Z')
    const rules = compileLifecycleEvaluationRules(
      {
        rules: [
          {
            status: 'Enabled',
            filter: {},
            noncurrentVersionExpiration: { noncurrentDays },
          },
        ],
      },
      snapshotAt
    )
    await expect(
      tHelper.database.evaluateNoncurrentLifecyclePage({
        bucketId,
        snapshotAt: snapshotAt.toISOString(),
        pageSize: 10,
        rules,
      })
    ).resolves.toEqual({ rawRowsExamined: 0, candidates: [], exhausted: true })
  })

  test('evaluates all rules in the database while raw-page progress stays selectivity-independent', async () => {
    await insertArchivedObjects([
      { name: 'old-a', archivedAt: '2026-07-01T00:00:00.000Z' },
      { name: 'old-b', archivedAt: '2026-07-02T00:00:00.000Z' },
      { name: 'young', archivedAt: '2026-08-10T00:00:00.000Z' },
    ])

    const page = await tHelper.database.evaluateNoncurrentLifecyclePage({
      bucketId,
      snapshotAt: '2026-08-15T12:00:00.000Z',
      pageSize: 500,
      rules: [{ cutoffAt: '2026-08-01T00:00:00.000Z' }, { cutoffAt: '2026-07-15T00:00:00.000Z' }],
    })

    expect(page.rawRowsExamined).toBe(2)
    expect(page.exhausted).toBe(true)
    expect(page.pageEnd).toBeDefined()
    expect(page.candidates.map((candidate) => candidate.name).sort()).toEqual(['old-a', 'old-b'])
    expect(new Set(page.candidates.map((candidate) => candidate.version)).size).toBe(2)
  })

  test('skips an archived row with an empty version instead of failing evaluation', async () => {
    await insertArchivedObjects([
      { name: 'blank-version', archivedAt: '2026-07-01T00:00:00.000Z', version: '' },
      { name: 'valid', archivedAt: '2026-07-02T00:00:00.000Z' },
    ])

    const page = await tHelper.database.evaluateNoncurrentLifecyclePage({
      bucketId,
      snapshotAt: '2026-08-15T12:00:00.000Z',
      pageSize: 500,
      rules: [{ cutoffAt: '2026-08-01T00:00:00.000Z' }],
    })

    expect(page.rawRowsExamined).toBe(2)
    expect(page.exhausted).toBe(true)
    expect(page.candidates.map((candidate) => candidate.name)).toEqual(['valid'])
  })

  test('preserves durable progress when releasing after an ambiguous write', async () => {
    const generation = await putConfiguration()
    const identity = { ...shard(bucketId), claimId: randomUUID() }
    await tHelper.database.claimLifecycleShard({ ...identity, leaseMs: 60_000 })
    const durable = newContinuation(generation)
    durable.counters.versionsExamined = 2
    durable.counters.versionsEligible = 1
    durable.counters.objectVersionsDeleted = 1
    durable.counters.bytesDeleted = 10
    durable.counters.batchesCompleted = 1
    await tHelper.database.releaseLifecycleShardClaim({
      ...identity,
      continuation: durable,
      nextRunAt: '2000-01-01T00:00:00.000Z',
    })
    await tHelper.database.claimLifecycleShard({ ...identity, leaseMs: 60_000 })

    const nextRunAt = '2099-01-01T00:00:00.000Z'
    await expect(
      tHelper.database.releaseLifecycleShardClaim({
        ...identity,
        nextRunAt,
        error: { message: 'commit acknowledgement lost' },
      })
    ).resolves.toBe(true)
    const rows = await fixtures.readState(
      bucketId,
      'continuation, claim_id, claim_until, next_run_at, failure_count, last_error'
    )
    expect(rows).toEqual([
      {
        continuation: durable,
        claim_id: null,
        claim_until: null,
        next_run_at: new Date(nextRunAt),
        failure_count: 1,
        last_error: { message: 'commit acknowledgement lost' },
      },
    ])
  })

  test('reconsiders an age-due row when a newer sibling later satisfies the retention count', async () => {
    const name = 'history/count-protected'
    const versions = [randomUUID(), randomUUID(), randomUUID()]
    const insertArchivedVersion = (version: string, archivedAt: string) =>
      insertArchivedObjects([{ name, version, archivedAt }])

    await insertArchivedVersion(versions[0], '2026-06-01T00:00:00.000Z')
    await insertArchivedVersion(versions[1], '2026-06-02T00:00:00.000Z')

    const evaluateFromRangeStart = () =>
      tHelper.database.evaluateNoncurrentLifecyclePage({
        bucketId,
        snapshotAt: '2026-08-15T12:00:00.000Z',
        pageSize: 500,
        rules: [
          {
            cutoffAt: '2026-08-01T00:00:00.000Z',
            newerNoncurrentVersions: 2,
          },
        ],
      })

    await expect(evaluateFromRangeStart()).resolves.toMatchObject({
      rawRowsExamined: 2,
      candidates: [],
    })

    await insertArchivedVersion(versions[2], '2026-06-03T00:00:00.000Z')

    const reconsidered = await evaluateFromRangeStart()
    expect(reconsidered.rawRowsExamined).toBe(3)
    expect(reconsidered.candidates).toEqual([
      expect.objectContaining({ name, version: versions[0] }),
    ])
  })

  test.each([
    1, 17, 500,
  ])('produces the same candidates and counters with page size %i', async (pageSize) => {
    await insertArchivedObjects(
      Array.from({ length: 17 }, (_, index) =>
        ['2026-06-01T00:00:00.000Z', '2026-06-02T00:00:00.000Z'].map((archivedAt) => ({
          name: `history/page-${index.toString().padStart(2, '0')}`,
          archivedAt,
        }))
      ).flat()
    )

    let cursor: NoncurrentLifecycleShardCursor | undefined
    let rawRowsExamined = 0
    const candidates: string[] = []
    for (;;) {
      const page = await tHelper.database.evaluateNoncurrentLifecyclePage({
        bucketId,
        snapshotAt: '2026-08-15T12:00:00.000Z',
        pageSize,
        rules: [
          {
            cutoffAt: '2026-08-01T00:00:00.000Z',
            newerNoncurrentVersions: 1,
          },
        ],
        ...(cursor ? { cursor } : {}),
      })
      rawRowsExamined += page.rawRowsExamined
      candidates.push(...page.candidates.map((candidate) => candidate.name))
      if (page.exhausted) break
      cursor = page.pageEnd
    }

    expect(rawRowsExamined).toBe(34)
    expect(candidates).toEqual(
      [...Array(17).keys()].map((index) => `history/page-${index.toString().padStart(2, '0')}`)
    )
  })

  describe('retained-version checks against explicit history fixtures', () => {
    async function stageCountProtectedVersion(
      options: {
        legacy?: boolean
        marker?: boolean
        metadata?: Record<string, unknown> | null
        suspended?: boolean
      } = {}
    ) {
      const generation = await putConfiguration(1)
      if (options.suspended) {
        await fixtures.setStatus(bucketId, 'SUSPENDED')
      }
      const name = 'history/retention-count-race'
      const olderId = randomUUID()
      const olderVersion = options.legacy ? null : randomUUID()
      const newerVersion = randomUUID()
      const writtenAt = '2000-01-01T00:00:00.000Z'
      // Explicit history fixtures written directly, not through a writer.
      await withServiceOperation('storage.object.upload', async (db) => {
        await db.query(
          `INSERT INTO storage.objects (
               id, bucket_id, name, version, created_at, archived_at, is_versioned, metadata, is_delete_marker
             ) VALUES
               ($1, $2, $3, $4, $6::timestamptz, '2001-01-01', $8, $7::jsonb, $9),
               (gen_random_uuid(), $2, $3, $5, $6::timestamptz, '2001-02-01', true, $10::jsonb, false)`,
          [
            olderId,
            bucketId,
            name,
            olderVersion,
            newerVersion,
            writtenAt,
            options.metadata === null ? null : JSON.stringify(options.metadata ?? { size: 10 }),
            !options.legacy,
            options.marker ?? false,
            JSON.stringify({ size: 10 }),
          ]
        )
      })
      const identity = { ...shard(bucketId), claimId: randomUUID() }
      await tHelper.database.claimLifecycleShard({ ...identity, leaseMs: 60_000 })
      const snapshotAt = '2001-06-01T00:00:00.000Z'
      const evaluation = {
        bucketId,
        snapshotAt,
        rules: [{ cutoffAt: '2001-05-01T00:00:00.000Z', newerNoncurrentVersions: 1 }],
        pageSize: 10,
      }
      const page = await tHelper.database.evaluateNoncurrentLifecyclePage(evaluation)
      expect(page.candidates).toEqual([
        { id: olderId, name, version: olderVersion, isDeleteMarker: options.marker ?? false },
      ])
      const armed = armedContinuation(generation, page.candidates, page.pageEnd!, {
        snapshotAt,
      })
      const arm = () =>
        tHelper.database.armLifecycleAttempt({
          ...identity,
          leaseMs: 60_000,
          continuation: armed,
        })
      return {
        identity,
        armed,
        arm,
        evaluation,
        name,
        olderVersion,
        newerVersion,
      }
    }

    test.each([
      { label: 'legacy null version', legacy: true, metadata: { size: 10 } },
      { label: 'version without extra metadata', metadata: { size: 10 } },
      { label: 'metadata-null marker', marker: true, metadata: null },
      { label: 'suspended history', suspended: true, metadata: { size: 10 } },
    ])('arms $label using the compatible row identity and policy', async ({
      label: _label,
      ...options
    }) => {
      const fixture = await stageCountProtectedVersion(options)
      await expect(fixture.arm()).resolves.toMatchObject({ continuation: fixture.armed })
    })

    test('rechecks retention at arm time after a raw fixture history change', async () => {
      const fixture = await stageCountProtectedVersion()
      await withServiceOperation('storage.lifecycle.test_fixture', async (transaction) => {
        await transaction.query(
          'DELETE FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
          [bucketId, fixture.name, fixture.newerVersion]
        )
      })
      // The arm path must independently discover that the retained-sibling count no longer holds.
      await expect(fixture.arm()).resolves.toBeUndefined()
      await expect(
        tHelper.database.revalidateLifecycleShardClaim(fixture.identity)
      ).resolves.toBeUndefined()
      await expect(
        tHelper.database.claimLifecycleShard({
          ...fixture.identity,
          claimId: randomUUID(),
          leaseMs: 60_000,
        })
      ).resolves.toMatchObject({ continuation: null })
      await expect(
        tHelper.database.evaluateNoncurrentLifecyclePage(fixture.evaluation)
      ).resolves.toMatchObject({ candidates: [] })
    })
  })
  it('commits partial artifact outcomes before advancing archived metadata', async () => {
    const generation = await putConfiguration()
    const name = 'history/a-expired.txt'
    const version = randomUUID()
    const id = randomUUID()
    const completedName = 'history/b-completed.txt'
    const completedVersion = randomUUID()
    const completedId = randomUUID()
    await fixtures.withServiceOperation('storage.s3.bucket.put_lifecycle', async (transaction) => {
      for (const [objectId, objectName, objectVersion, size] of [
        [id, name, version, 42],
        [completedId, completedName, completedVersion, 8],
      ]) {
        await transaction.query(
          `INSERT INTO storage.objects (id, bucket_id, name, version, metadata, archived_at, is_versioned)
           VALUES ($1, $2, $3, $4, $5::jsonb, '2026-07-02', true)`,
          [objectId, bucketId, objectName, objectVersion, JSON.stringify({ size })]
        )
      }
    })

    const claimId = randomUUID()
    const claimed = await tHelper.database.claimLifecycleShard({
      ...shard(bucketId),
      claimId,
      leaseMs: 120_000,
    })
    expect(claimed).toMatchObject({ claimId })
    const candidate = {
      id,
      name,
      version,
      isDeleteMarker: false,
      archivedAt: '2026-07-02T00:00:00.000Z',
    }
    const completedCandidate = {
      ...candidate,
      id: completedId,
      name: completedName,
      version: completedVersion,
    }
    const armed = armedContinuation(
      generation,
      [candidate, completedCandidate],
      { name: completedName, archivedAt: completedCandidate.archivedAt },
      { snapshotAt: '2026-09-01T00:00:00.000Z' }
    )
    const armedState = await tHelper.database.armLifecycleAttempt({
      ...shard(bucketId),
      claimId,
      leaseMs: 120_000,
      continuation: armed,
    })
    expect(armedState?.continuation).toMatchObject({
      batch: { inFlight: { attemptId: armed.batch!.inFlight!.attemptId } },
    })

    const partiallyRecorded = recordLifecycleArtifactOutcomes(
      armed,
      new Map<string, { outcome: 'DELETED' | 'FAILED'; error?: string }>([
        [armed.batch!.versions[0].artifacts[0].key, { outcome: 'DELETED' as const }],
        [
          armed.batch!.versions[0].artifacts[1].key,
          { outcome: 'FAILED' as const, error: 'retryable backend failure' },
        ],
        ...armed.batch!.versions[1].artifacts.map(
          (artifact) => [artifact.key, { outcome: 'DELETED' as const }] as const
        ),
      ])
    )
    const partial = await tHelper.database.commitLifecycleAttempt({
      ...shard(bucketId),
      claimId,
      attemptId: armed.batch!.inFlight!.attemptId,
      continuation: partiallyRecorded,
    })
    expect(partial?.continuation).toMatchObject({
      batch: {
        inFlight: { attemptId: armed.batch!.inFlight!.attemptId },
        versions: [
          {
            artifacts: [
              expect.objectContaining({ outcome: 'DELETED' }),
              expect.objectContaining({ outcome: 'FAILED' }),
            ],
          },
        ],
      },
      counters: { objectVersionsDeleted: 1, bytesDeleted: 8, batchesCompleted: 0 },
    })
    expect(
      await tHelper.database.connection.query(
        'SELECT 1 FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
        [bucketId, name, version]
      )
    ).toMatchObject({ rowCount: 1 })
    expect(
      await tHelper.database.connection.query(
        'SELECT 1 FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
        [bucketId, completedName, completedVersion]
      )
    ).toMatchObject({ rowCount: 0 })

    const recorded = recordLifecycleArtifactOutcomes(
      partial!.continuation!,
      new Map([[armed.batch!.versions[0].artifacts[1].key, { outcome: 'DELETED' as const }]])
    )
    const committed = await tHelper.database.commitLifecycleAttempt({
      ...shard(bucketId),
      claimId,
      attemptId: armed.batch!.inFlight!.attemptId,
      continuation: recorded,
    })
    expect(committed?.continuation).toMatchObject({
      cursor: { name: completedName, archivedAt: candidate.archivedAt },
      counters: { objectVersionsDeleted: 2, bytesDeleted: 50, batchesCompleted: 1 },
    })
    expect(
      await tHelper.database.connection.query(
        'SELECT 1 FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
        [bucketId, name, version]
      )
    ).toMatchObject({ rowCount: 0 })
  })
  describe('shard queries', () => {
    const claim = (id = bucketId, leaseMs = 60_000) =>
      tHelper.database.claimLifecycleShard({ ...shard(id), claimId: randomUUID(), leaseMs })
    const setState = (sets: string, id = bucketId, values: unknown[] = []) =>
      tHelper.database.connection.query(
        `UPDATE storage.bucket_lifecycle_states SET ${sets} WHERE bucket_id = $1`,
        [id, ...values]
      )
    const listed = async (id: string) =>
      (await tHelper.database.listDueLifecycleShards(1000)).some(
        (coordinate) => coordinate.bucketId === id
      )

    test('excludes a live claim from the due list and from the next dispatch time', async () => {
      await putConfiguration()
      expect(await listed(bucketId)).toBe(true)
      await expect(claim()).resolves.toBeDefined()

      expect(await listed(bucketId)).toBe(false)
      const { nextDispatchAt: next } = await tHelper.database.findLifecycleDispatchState()
      expect(next === null || next.getTime() > Date.now()).toBe(true)

      await setState("claim_until = now() - interval '1 second'")
      expect(await listed(bucketId)).toBe(true)
    })

    test('reports a claimed shard at its lease expiry rather than as due', async () => {
      await putConfiguration()
      await setState("next_run_at = '2000-01-01'")
      const lease = new Date(Date.now() + 3_600_000)
      await setState(`claim_id = gen_random_uuid(), claim_until = '${lease.toISOString()}'`)

      const { nextDispatchAt: next } = await tHelper.database.findLifecycleDispatchState()
      expect(next).not.toBeNull()
      expect(next!.getTime()).toBeGreaterThanOrEqual(lease.getTime() - 1000)
    })

    test('cannot claim a shard that is not yet due', async () => {
      await putConfiguration()
      await setState("next_run_at = now() + interval '1 hour'")
      expect(await listed(bucketId)).toBe(false)
      await expect(claim()).resolves.toBeUndefined()
    })

    test('neither lists nor claims a shard with a stale epoch', async () => {
      await putConfiguration()
      await setState('shard_epoch = shard_epoch + 1')
      expect(await listed(bucketId)).toBe(false)
      await expect(claim()).resolves.toBeUndefined()
    })

    test('stops revalidating a claim once its lease has passed', async () => {
      await putConfiguration()
      const claimId = randomUUID()
      await tHelper.database.claimLifecycleShard({ ...shard(bucketId), claimId, leaseMs: 60_000 })
      await expect(
        tHelper.database.revalidateLifecycleShardClaim({ ...shard(bucketId), claimId })
      ).resolves.toMatchObject({ claimId })

      await setState("claim_until = now() - interval '1 second'")
      await expect(
        tHelper.database.revalidateLifecycleShardClaim({ ...shard(bucketId), claimId })
      ).resolves.toBeUndefined()
    })

    test('restricts due shards and the next dispatch time to journals in flight on request', async () => {
      const other = await fixtures.createBucket()
      for (const id of [bucketId, other]) {
        await fixtures.setStatus(id, 'ENABLED')
        await tHelper.database.putLifecycleConfiguration(id, {
          rules: [
            { status: 'Enabled', filter: {}, noncurrentVersionExpiration: { noncurrentDays: 30 } },
          ],
        })
      }
      await setState("next_run_at = '2000-01-01'", other)
      await setState("next_run_at = '2000-01-02'", bucketId)
      const generation = (await tHelper.database.findLifecycleBucket(bucketId))
        .lifecycle_configuration_generation!
      const continuation = armedContinuation(
        generation,
        [{ id: randomUUID(), name: 'a', version: randomUUID(), isDeleteMarker: false }],
        { name: 'a', archivedAt: new Date().toISOString() }
      )
      await setState('continuation = $2::jsonb', bucketId, [JSON.stringify(continuation)])

      const all = (await tHelper.database.listDueLifecycleShards(1000)).map((c) => c.bucketId)
      expect(all).toEqual(expect.arrayContaining([bucketId, other]))
      expect(all.indexOf(other)).toBeLessThan(all.indexOf(bucketId))
      const filtered = (
        await tHelper.database.listDueLifecycleShards(1000, { inFlightOnly: true })
      ).map((c) => c.bucketId)
      expect(filtered).toContain(bucketId)
      expect(filtered).not.toContain(other)

      const { nextDispatchAt: next } = await tHelper.database.findLifecycleDispatchState({
        inFlightOnly: true,
      })
      expect(next).toEqual(new Date('2000-01-02'))
      await expect(tHelper.database.findLifecycleDispatchState()).resolves.toMatchObject({
        nextDispatchAt: new Date('2000-01-01'),
      })
    })
  })

  test('reports parked failures and in-flight age without decoding journals', async () => {
    await putConfiguration()
    const other = await fixtures.createBucket()
    await tHelper.database.putLifecycleConfiguration(other, {
      rules: [
        { status: 'Enabled', filter: {}, noncurrentVersionExpiration: { noncurrentDays: 1 } },
      ],
    })
    await tHelper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states SET next_run_at = NULL, failure_count = 9 WHERE bucket_id = $1`,
      [bucketId]
    )
    await tHelper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states SET next_run_at = '2026-01-01', failure_count = 3,
         continuation = '{"continuationVersion":999,"batch":{"inFlight":{"startedAt":"2025-12-01T00:00:00.000Z"}}}'
       WHERE bucket_id = $1`,
      [other]
    )
    await expect(
      tHelper.database.findLifecycleDispatchState({ inFlightOnly: true })
    ).resolves.toEqual({
      nextDispatchAt: new Date('2026-01-01'),
      maxFailureCount: 9,
      oldestInFlightStartedAt: '2025-12-01T00:00:00.000Z',
    })
    await tHelper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states SET continuation = jsonb_set(continuation, '{batch,inFlight,startedAt}', '"invalid"') WHERE bucket_id = $1`,
      [other]
    )
    await expect(tHelper.database.findLifecycleDispatchState()).resolves.toMatchObject({
      nextDispatchAt: new Date('2026-01-01'),
      maxFailureCount: 9,
      oldestInFlightStartedAt: null,
    })
  })

  describe('arm and commit coordination', () => {
    async function armBatch(
      names: string[],
      options: { archivedAt?: string; snapshotAt?: string; newerNoncurrentVersions?: number } = {}
    ) {
      const generation = await putConfiguration(options.newerNoncurrentVersions)
      const rows = names.map((name) => ({
        id: randomUUID(),
        name,
        version: randomUUID(),
        archivedAt: options.archivedAt ?? '2026-06-01T00:00:00.000Z',
      }))
      await insertArchivedObjects(rows)
      const identity = { ...shard(bucketId), claimId: randomUUID() }
      await tHelper.database.claimLifecycleShard({ ...identity, leaseMs: 60_000 })
      const staged = stageLifecycleBatch(
        newContinuation(generation, options.snapshotAt ? { snapshotAt: options.snapshotAt } : {}),
        {
          rawRowsExamined: rows.length,
          pageEnd: { name: rows.at(-1)!.name, archivedAt: rows.at(-1)!.archivedAt },
          versions: rows.map(({ id, name, version }) =>
            freezeLifecycleBatchVersion({ id, name, version, isDeleteMarker: false })
          ),
        }
      )
      const armed = armLifecycleAttempt(staged, {
        attemptId: randomUUID(),
        startedAt: new Date().toISOString(),
      })
      const input = {
        ...identity,
        leaseMs: 60_000,
        continuation: armed,
      }
      const recorded = () =>
        recordLifecycleArtifactOutcomes(
          armed,
          new Map(
            armed.batch!.versions.flatMap((version) =>
              version.artifacts.map((artifact) => [artifact.key, { outcome: 'DELETED' as const }])
            )
          )
        )
      return {
        identity,
        rows,
        generation,
        staged,
        armed,
        input,
        arm: () => tHelper.database.armLifecycleAttempt(input),
        commit: (continuation = recorded()) =>
          tHelper.database.commitLifecycleAttempt({
            ...identity,
            attemptId: armed.batch!.inFlight!.attemptId,
            continuation,
          }),
        recorded,
      }
    }

    test.each([
      { state: 'armed', explicit: false, capped: true },
      { state: 'armed', explicit: true, capped: false },
      { state: 'unreadable armed', explicit: false, capped: true },
      { state: 'unreadable idle', explicit: false, capped: false },
      { state: 'empty', explicit: false, capped: false },
    ])('caps only preserved journals on release: $state, explicit=$explicit', async ({
      state,
      explicit,
      capped,
    }) => {
      const fixture = await armBatch(['retry-cap'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const continuation =
        state === 'armed'
          ? fixture.armed
          : state === 'empty'
            ? null
            : {
                continuationVersion: 999,
                ...(state === 'unreadable armed' && {
                  batch: { inFlight: { attemptId: randomUUID() } },
                }),
              }
      await tHelper.database.connection.query(
        `UPDATE storage.bucket_lifecycle_states
         SET continuation = $2::jsonb, claim_until = '2000-01-01' WHERE bucket_id = $1`,
        [bucketId, continuation === null ? null : JSON.stringify(continuation)]
      )
      const nextRunAt = '2099-01-01T01:00:00.000Z'
      const maxInFlightRetryAt = '2099-01-01T00:15:00.000Z'
      await expect(
        tHelper.database.releaseLifecycleShardClaim({
          ...fixture.identity,
          // Explicit journals include recovery-grace schedules and must not be capped.
          ...(explicit && { continuation: fixture.armed }),
          nextRunAt,
          maxInFlightRetryAt,
          error: { message: 'lost acknowledgement' },
        })
      ).resolves.toBe(true)
      await expect(
        fixtures.readState(bucketId, 'continuation, claim_id, next_run_at')
      ).resolves.toEqual([
        {
          continuation,
          claim_id: null,
          next_run_at: new Date(capped ? maxInFlightRetryAt : nextRunAt),
        },
      ])
    })

    const expireClaim = () =>
      tHelper.database.connection.query(
        "UPDATE storage.bucket_lifecycle_states SET claim_until = now() - interval '1 second' WHERE bucket_id = $1",
        [bucketId]
      )

    test('rejects arming once the claim lease has expired', async () => {
      const fixture = await armBatch(['arm/expired'])
      await expireClaim()

      await expect(fixture.arm()).resolves.toBeUndefined()
      await expect(fixtures.readState(bucketId, 'continuation')).resolves.toEqual([
        { continuation: null },
      ])
    })

    test('rejects arming while a journal is already in flight', async () => {
      const fixture = await armBatch(['arm/twice'])
      await expect(fixture.arm()).resolves.toBeDefined()

      await expect(fixture.arm()).resolves.toBeUndefined()
      await expect(fixtures.readState(bucketId, 'continuation')).resolves.toEqual([
        { continuation: fixture.armed },
      ])
    })

    test('revalidates a recovery attempt only while the claim lease is live', async () => {
      const fixture = await armBatch(['arm/recovery'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const attemptId = fixture.armed.batch!.inFlight!.attemptId

      await expect(
        tHelper.database.revalidateLifecycleRecoveryAttempt(fixture.identity, attemptId, 60_000)
      ).resolves.toBeDefined()
      await expireClaim()
      await expect(
        tHelper.database.revalidateLifecycleRecoveryAttempt(fixture.identity, attemptId, 60_000)
      ).resolves.toBeUndefined()
    })

    const SNAPSHOT_AT = '2026-08-15T00:00:00.000Z'
    test.each([
      { label: 'exactly at the rule cutoff', archivedAt: '2026-07-16T00:00:00.000Z', arms: false },
      { label: 'just before the rule cutoff', archivedAt: '2026-07-15T23:59:59.999Z', arms: true },
    ])('arms a row archived $label: $arms', async ({ archivedAt, arms }) => {
      const fixture = await armBatch(['arm/cutoff'], { archivedAt, snapshotAt: SNAPSHOT_AT })

      const armed = await fixture.arm()
      expect(armed !== undefined).toBe(arms)
      if (!arms) {
        await expect(fixtures.readState(bucketId, 'continuation, claim_id')).resolves.toEqual([
          { continuation: null, claim_id: null },
        ])
      }
    })

    test.each([
      {
        label: 'a sibling archived after the snapshot',
        newer: 1,
        siblings: ['2026-09-01'],
        arms: false,
      },
      {
        label: 'a sibling archived before the snapshot',
        newer: 1,
        siblings: ['2026-07-01'],
        arms: true,
      },
      { label: 'too few newer siblings', newer: 2, siblings: ['2026-07-01'], arms: false },
      {
        label: 'more newer siblings than the count',
        newer: 2,
        siblings: ['2026-07-01', '2026-07-02', '2026-07-03'],
        arms: true,
      },
    ])('applies newerNoncurrentVersions at arm time with $label', async ({
      newer,
      siblings,
      arms,
    }) => {
      const fixture = await armBatch(['arm/newer'], {
        newerNoncurrentVersions: newer,
        snapshotAt: SNAPSHOT_AT,
      })
      await insertArchivedObjects(siblings.map((archivedAt) => ({ name: 'arm/newer', archivedAt })))

      expect((await fixture.arm()) !== undefined).toBe(arms)
    })

    const rowExists = async ({ name, version }: { name: string; version: string }) =>
      (
        await tHelper.database.connection.query(
          'SELECT 1 FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
          [bucketId, name, version]
        )
      ).rowCount === 1

    async function holdWriter(sql: string, values: unknown[], setup?: string) {
      const writer = new Client({ connectionString: getConfig().databaseURL })
      await writer.connect()
      await writer.query('BEGIN')
      const pid = (await writer.query('SELECT pg_backend_pid() AS pid')).rows[0].pid
      if (setup) await writer.query(setup)
      await writer.query(sql, values)
      return {
        writer,
        pid,
        async finish(action: 'COMMIT' | 'ROLLBACK') {
          await writer.query(action)
          await writer.end()
        },
        blocked: () =>
          expect
            .poll(
              async () =>
                (
                  await tHelper.database.connection.query(
                    'SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))) AS blocked',
                    [pid]
                  )
                ).rows[0].blocked
            )
            .toBe(true),
      }
    }

    test('rejects a stale attempt before deleting any metadata', async () => {
      const fixture = await armBatch(['commit/stale-attempt'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const continuation = fixture.recorded()
      const attemptId = randomUUID()
      continuation.batch!.inFlight!.attemptId = attemptId
      await expect(
        tHelper.database.commitLifecycleAttempt({
          ...fixture.identity,
          attemptId,
          continuation,
        })
      ).resolves.toBeUndefined()
      expect(await rowExists(fixture.rows[0])).toBe(true)
      await expect(fixtures.readState(bucketId, 'continuation')).resolves.toEqual([
        { continuation: fixture.armed },
      ])
    })

    const mutations = {
      renamed: () => ['UPDATE storage.objects SET name = name || $2 WHERE id = $1', '-moved'],
      reversioned: () => ['UPDATE storage.objects SET version = $2 WHERE id = $1', randomUUID()],
      replaced: () => [
        `WITH old AS (DELETE FROM storage.objects WHERE id = $1 RETURNING *)
         INSERT INTO storage.objects (id, bucket_id, name, version, archived_at, is_versioned, metadata)
         SELECT $2, bucket_id, name, version, archived_at, is_versioned, metadata FROM old`,
        randomUUID(),
      ],
    } as const

    async function mutateRow(kind: keyof typeof mutations, id: string) {
      const [sql, param] = mutations[kind]()
      await withServiceOperation('storage.lifecycle.test_fixture', (transaction) =>
        transaction.query(sql, [id, param])
      )
      return kind === 'replaced' ? param : id
    }

    test.each(
      Object.keys(mutations) as Array<keyof typeof mutations>
    )('does not find or arm a row that was %s after staging', async (kind) => {
      const fixture = await armBatch(['identity/pre-arm'])
      const [row] = fixture.rows
      await mutateRow(kind, row.id)

      await expect(tHelper.database.findLifecycleObjectVersions(bucketId, [row])).resolves.toEqual(
        []
      )
      await expect(fixture.arm()).resolves.toBeUndefined()
    })

    test.each(
      Object.keys(mutations) as Array<keyof typeof mutations>
    )('does not delete a row that was %s after arming', async (kind) => {
      const fixture = await armBatch(['identity/post-arm'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const survivorId = await mutateRow(kind, fixture.rows[0].id)

      await fixture.commit()
      await expect(
        tHelper.database.connection.query('SELECT 1 FROM storage.objects WHERE id = $1', [
          survivorId,
        ])
      ).resolves.toMatchObject({ rowCount: 1 })
    })

    test('finds a legacy null-version row only by its id', async () => {
      const row = { id: randomUUID(), name: 'identity/legacy', version: null }
      await tHelper.database.connection.query(
        `INSERT INTO storage.objects (id, bucket_id, name, version, archived_at, metadata)
         VALUES ($1, $2, $3, NULL, now(), '{"size": 10}')`,
        [row.id, bucketId, row.name]
      )
      await expect(tHelper.database.findLifecycleObjectVersions(bucketId, [row])).resolves.toEqual([
        row,
      ])
      await expect(
        tHelper.database.findLifecycleObjectVersions(bucketId, [{ ...row, id: randomUUID() }])
      ).resolves.toEqual([])
    })

    test('rechecks a replaced claim under the state lock before deleting metadata', async () => {
      const fixture = await armBatch(['commit/replaced-claim'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const held = await holdWriter(
        'SELECT 1 FROM storage.bucket_lifecycle_states WHERE bucket_id = $1 FOR UPDATE',
        [bucketId]
      )
      try {
        const committing = fixture.commit()
        await held.blocked()
        await held.writer.query(
          'UPDATE storage.bucket_lifecycle_states SET claim_id = gen_random_uuid() WHERE bucket_id = $1',
          [bucketId]
        )
        await held.finish('COMMIT')
        await expect(committing).resolves.toBeUndefined()
        expect(await rowExists(fixture.rows[0])).toBe(true)
      } finally {
        await held.writer.end().catch(() => undefined)
      }
    })

    test('rejects a commit whose version or artifact set differs from the armed batch', async () => {
      const fixture = await armBatch(['commit/a', 'commit/b'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const recorded = fixture.recorded()

      const dropped = {
        ...recorded,
        batch: { ...recorded.batch!, versions: recorded.batch!.versions.slice(0, 1) },
      }
      const rekeyed = {
        ...recorded,
        batch: {
          ...recorded.batch!,
          versions: recorded.batch!.versions.map((version, index) =>
            index === 0
              ? {
                  ...version,
                  artifacts: version.artifacts.map((artifact) => ({
                    ...artifact,
                    key: `${artifact.key}-other`,
                  })),
                }
              : version
          ),
        },
      }
      for (const proposed of [dropped, rekeyed]) {
        await expect(fixture.commit(proposed)).rejects.toMatchObject({
          code: 'InvalidRequest',
          message: expect.stringContaining('artifact set changed'),
        })
      }
      expect(await Promise.all(fixture.rows.map(rowExists))).toEqual([true, true])
      await expect(fixture.commit()).resolves.toBeDefined()
      expect(await Promise.all(fixture.rows.map(rowExists))).toEqual([false, false])
    })

    test('waits on a writer row lock at commit and then deletes the rows', async () => {
      const fixture = await armBatch(['commit/wait'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const held = await holdWriter(
        'SELECT 1 FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3 FOR UPDATE',
        [bucketId, fixture.rows[0].name, fixture.rows[0].version]
      )
      try {
        const committing = fixture.commit()
        await held.blocked()
        expect(await rowExists(fixture.rows[0])).toBe(true)
        await held.finish('ROLLBACK')
        await expect(committing).resolves.toBeDefined()
        expect(await rowExists(fixture.rows[0])).toBe(false)
      } finally {
        await held.writer.end().catch(() => undefined)
      }
    })

    test('waits on a writer holding the object advisory lock at commit', async () => {
      const fixture = await armBatch(['commit/advisory'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const acquired = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      const holding = tHelper.database.withTransaction(async (db) => {
        await db.waitObjectLocks([{ bucketId, objectName: fixture.rows[0].name }])
        acquired.resolve()
        await release.promise
      })
      await acquired.promise
      const committing = fixture.commit()
      try {
        await expect
          .poll(
            async () =>
              (
                await tHelper.database.connection.query(
                  `SELECT count(*)::int AS waiting FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`
                )
              ).rows[0].waiting
          )
          .toBeGreaterThan(0)
        expect(await rowExists(fixture.rows[0])).toBe(true)
      } finally {
        release.resolve()
        await holding
      }
      await expect(committing).resolves.toBeDefined()
      expect(await rowExists(fixture.rows[0])).toBe(false)
    })

    test.each([
      'name',
      'version',
      'bucket_id',
    ] as const)('does not delete a row whose %s changes while commit waits for its row lock', async (column) => {
      const fixture = await armBatch(['commit/changed'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const replacement = column === 'bucket_id' ? await fixtures.createBucket() : randomUUID()
      const held = await holdWriter(
        `UPDATE storage.objects SET ${column} = $4 WHERE bucket_id = $1 AND name = $2 AND version = $3`,
        [bucketId, fixture.rows[0].name, fixture.rows[0].version, replacement]
      )
      try {
        const committing = fixture.commit()
        await held.blocked()
        await held.finish('COMMIT')
        const committed = await committing
        expect(committed?.continuation?.counters.objectVersionsDeleted).toBe(0)
        const rows = await tHelper.database.connection.query(
          `SELECT 1 FROM storage.objects WHERE ${column} = $1`,
          [replacement]
        )
        expect(rows.rowCount).toBe(1)
      } finally {
        await held.writer.end().catch(() => undefined)
      }
    })

    test('does not delete a row promoted before the commit took its row lock', async () => {
      const fixture = await armBatch(['commit/promoted'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const held = await holdWriter(
        'UPDATE storage.objects SET archived_at = NULL WHERE bucket_id = $1 AND name = $2 AND version = $3',
        [bucketId, fixture.rows[0].name, fixture.rows[0].version],
        'SET LOCAL session_replication_role = replica'
      )
      try {
        const committing = fixture.commit()
        await held.blocked()
        await held.finish('COMMIT')
        await expect(committing).resolves.toBeDefined()
        const [row] = (
          await tHelper.database.connection.query(
            'SELECT archived_at FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
            [bucketId, fixture.rows[0].name, fixture.rows[0].version]
          )
        ).rows
        expect(row).toEqual({ archived_at: null })
      } finally {
        await held.writer.end().catch(() => undefined)
      }
    })

    test.each([
      { holder: 'object advisory lock' },
      { holder: 'bucket row lock' },
      { holder: 'object row lock' },
    ])('fails arm with LockTimeout after about 5s behind a $holder', async ({ holder }) => {
      const fixture = await armBatch(['lock/a'])
      const release = Promise.withResolvers<void>()
      let holding: Promise<unknown> | undefined
      let writer: Awaited<ReturnType<typeof holdWriter>> | undefined
      if (holder === 'object advisory lock') {
        const acquired = Promise.withResolvers<void>()
        holding = tHelper.database.withTransaction(async (db) => {
          await db.waitObjectLocks([{ bucketId, objectName: fixture.rows[0].name }])
          acquired.resolve()
          await release.promise
        })
        await acquired.promise
      } else if (holder === 'bucket row lock') {
        writer = await holdWriter('SELECT 1 FROM storage.buckets WHERE id = $1 FOR UPDATE', [
          bucketId,
        ])
      } else {
        writer = await holdWriter(
          'SELECT 1 FROM storage.objects WHERE bucket_id = $1 AND name = $2 FOR UPDATE',
          [bucketId, fixture.rows[0].name]
        )
      }
      const startedAt = Date.now()
      try {
        await expect(fixture.arm()).rejects.toMatchObject({
          code: 'LockTimeout',
          httpStatusCode: 503,
        })
        expect(Date.now() - startedAt).toBeGreaterThan(4_500)
        expect(Date.now() - startedAt).toBeLessThan(12_000)
      } finally {
        release.resolve()
        await holding
        await writer?.finish('ROLLBACK')
      }
    }, 30_000)

    test('arm waits for a writer holding the object advisory lock, then proceeds', async () => {
      const fixture = await armBatch(['lock/wait'])
      const acquired = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      const holding = tHelper.database.withTransaction(async (db) => {
        await db.waitObjectLocks([{ bucketId, objectName: fixture.rows[0].name }])
        acquired.resolve()
        await release.promise
      })
      await acquired.promise
      const arming = fixture.arm()
      await expect
        .poll(
          async () =>
            (
              await tHelper.database.connection.query(
                `SELECT count(*)::int AS waiting FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`
              )
            ).rows[0].waiting
        )
        .toBeGreaterThan(0)
      release.resolve()
      await holding
      await expect(arming).resolves.toBeDefined()
    })

    describe('promotion fence precision', () => {
      const promote = (id: string, name: string, version: string) =>
        tHelper.database.connection.query(
          'UPDATE storage.objects SET archived_at = NULL WHERE bucket_id = $1 AND name = $2 AND version = $3',
          [id, name, version]
        )

      test('blocks promotion of the journaled version only', async () => {
        const fixture = await armBatch(['fence/a'])
        await expect(fixture.arm()).resolves.toBeDefined()
        const other = randomUUID()
        await insertArchivedObjects([
          { name: 'fence/a', archivedAt: '2026-06-02T00:00:00.000Z', version: other },
        ])

        await expect(promote(bucketId, 'fence/a', other)).resolves.toBeDefined()
        await expect(
          promote(bucketId, fixture.rows[0].name, fixture.rows[0].version)
        ).rejects.toMatchObject({ code: '55P03' })
      })

      test('does not block promotion of a staged batch that is not armed', async () => {
        const fixture = await armBatch(['fence/staged'])
        await tHelper.database.connection.query(
          'UPDATE storage.bucket_lifecycle_states SET continuation = $2::jsonb WHERE bucket_id = $1',
          [bucketId, JSON.stringify(fixture.staged)]
        )

        await expect(
          promote(bucketId, fixture.rows[0].name, fixture.rows[0].version)
        ).resolves.toBeDefined()
      })

      test('does not extend a journal to the same name and version in another bucket', async () => {
        const fixture = await armBatch(['fence/shared'])
        await expect(fixture.arm()).resolves.toBeDefined()
        const other = await fixtures.createBucket()
        await tHelper.database.connection.query(
          `INSERT INTO storage.objects (bucket_id, name, version, is_versioned, metadata, archived_at)
           VALUES ($1, $2, $3, true, '{"size":1}', '2026-06-01')`,
          [other, fixture.rows[0].name, fixture.rows[0].version]
        )

        await expect(
          promote(other, fixture.rows[0].name, fixture.rows[0].version)
        ).resolves.toBeDefined()
      })

      test('does not fire for an update that keeps the row archived', async () => {
        const fixture = await armBatch(['fence/archived'])
        await expect(fixture.arm()).resolves.toBeDefined()
        await expect(
          tHelper.database.connection.query(
            `UPDATE storage.objects SET archived_at = archived_at + interval '1 minute'
             WHERE bucket_id = $1 AND name = $2 AND version = $3`,
            [bucketId, fixture.rows[0].name, fixture.rows[0].version]
          )
        ).resolves.toMatchObject({ rowCount: 1 })
      })
    })

    test('holds a bucket share lock during arming that blocks bucket deletion', async () => {
      const fixture = await armBatch(['lock/share'])
      const deleter = new Client({ connectionString: getConfig().databaseURL })
      await deleter.connect()
      let deleting: Promise<unknown> | undefined
      try {
        await tHelper.database.withTransaction(async (db) => {
          await expect(db.armLifecycleAttempt(fixture.input)).resolves.toBeDefined()
          await deleter.query('BEGIN')
          await deleter.query("SELECT set_config('storage.allow_delete_query', 'true', true)")
          const pid = (await deleter.query('SELECT pg_backend_pid() AS pid')).rows[0].pid
          deleting = deleter
            .query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
            .catch((error: unknown) => error)
          await expect
            .poll(
              async () =>
                (
                  await tHelper.database.connection.query(
                    'SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked',
                    [pid]
                  )
                ).rows[0].blocked
            )
            .toBe(true)
        })
        await deleting
        await deleter.query('ROLLBACK')
      } finally {
        await deleter.end()
      }
    })
  })

  test('locks id-batch deletions in the same name order as arming', async () => {
    const names = ['order/a', 'order/b', 'order/c']
    const ids = [
      'ffffffff-0000-4000-8000-000000000000',
      '88888888-0000-4000-8000-000000000000',
      '00000000-0000-4000-8000-000000000000',
    ]
    const version = () => randomUUID()
    await tHelper.database.connection.query(
      `INSERT INTO storage.objects (id, bucket_id, name, version, is_versioned, metadata)
       SELECT id::uuid, $1, name, v, true, '{"size":1}'
       FROM unnest($2::text[], $3::text[], $4::text[]) WITH ORDINALITY AS t(name, v, id, i)
       ORDER BY i DESC`,
      [bucketId, names, names.map(version), ids]
    )
    // Enough rows for the planner to scan by id instead of by name.
    await tHelper.database.connection.query(
      `INSERT INTO storage.objects (bucket_id, name, version, is_versioned, metadata)
       SELECT $1, 'filler/' || g, gen_random_uuid()::text, true, '{"size":1}'
       FROM generate_series(1, 20000) AS g`,
      [bucketId]
    )
    await tHelper.database.connection.query('ANALYZE storage.objects')
    const holder = new Client({ connectionString: getConfig().databaseURL })
    const observer = new Client({ connectionString: getConfig().databaseURL })
    await holder.connect()
    await observer.connect()
    let deleting: Promise<unknown> | undefined
    try {
      await holder.query('BEGIN')
      await holder.query('SELECT 1 FROM storage.objects WHERE id = $1 FOR UPDATE', [ids[1]])
      deleting = tHelper.database.withTransaction((db) => db.deleteObjects(bucketId, ids, 'id'))
      await expect
        .poll(
          async () =>
            (
              await observer.query(
                'SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))) AS blocked',
                [(await holder.query('SELECT pg_backend_pid() AS pid')).rows[0].pid]
              )
            ).rows[0].blocked
        )
        .toBe(true)
      const free = (
        await observer.query(
          'SELECT name FROM storage.objects WHERE bucket_id = $1 FOR UPDATE SKIP LOCKED',
          [bucketId]
        )
      ).rows.map((row) => row.name)
      expect(free).not.toContain('order/a')
      expect(free).toContain('order/c')
    } finally {
      await holder.query('ROLLBACK')
      await deleting
      await holder.end()
      await observer.end()
    }
  })
})
