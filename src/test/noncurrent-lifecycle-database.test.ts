import { randomUUID } from 'node:crypto'
import type { DatabaseTransaction } from '@internal/database'
import { compileLifecycleEvaluationRules } from '@storage/lifecycle/configuration'
import {
  armLifecycleAttempt,
  createLifecycleContinuation,
  freezeLifecycleBatchVersion,
  stageLifecycleBatch,
} from '@storage/lifecycle/continuation'
import type { NoncurrentLifecycleShardCursor } from '@storage/schemas'
import { useLifecycleVersioningFixtures } from './utils/lifecycle-versioning'
import { useStorage } from './utils/storage'

describe('noncurrent lifecycle database API', () => {
  const tHelper = useStorage()
  const fixtures = useLifecycleVersioningFixtures()
  let bucketId: string
  let analyticsBucketId: string

  beforeEach(async () => {
    bucketId = fixtures.trackBucket(`lifecycle-db-${randomUUID()}`)
    analyticsBucketId = fixtures.trackBucket(`lifecycle-db-analytics-${randomUUID()}`)
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })
    await tHelper.database.createBucket({ id: analyticsBucketId, name: analyticsBucketId })
    await tHelper.database.connection.query(
      `UPDATE storage.buckets SET type = 'ANALYTICS' WHERE id = $1`,
      [analyticsBucketId]
    )
  })

  afterEach(async () => {
    const transaction = await tHelper.database.connection.transaction()
    try {
      await transaction.query(`SELECT set_config('storage.allow_delete_query', 'true', true)`)

      await transaction.query(
        `DELETE FROM storage.bucket_lifecycle_states WHERE bucket_id = ANY($1::text[])`,
        [[bucketId, analyticsBucketId]]
      )
      await transaction.query(`DELETE FROM storage.objects WHERE bucket_id = ANY($1::text[])`, [
        [bucketId, analyticsBucketId],
      ])
      await transaction.query(`DELETE FROM storage.buckets WHERE id = ANY($1::text[])`, [
        [bucketId, analyticsBucketId],
      ])
      await transaction.commit()
    } catch (error) {
      await transaction.rollback()
      throw error
    }
  })

  async function withServiceOperation<T>(
    operation: string,
    fn: (transaction: DatabaseTransaction) => Promise<T>
  ): Promise<T> {
    const transaction = await tHelper.database.connection.transaction()
    try {
      await tHelper.database.connection.setScope(transaction)
      await transaction.query(`SELECT set_config('storage.operation', $1, true)`, [operation])
      const result = await fn(transaction)
      await transaction.commit()
      return result
    } catch (error) {
      await transaction.rollback()
      throw error
    }
  }

  async function putConfiguration(newerNoncurrentVersions?: number) {
    const generation = randomUUID()
    await withServiceOperation('storage.s3.bucket.put_lifecycle', async (transaction) => {
      await transaction.query(
        `UPDATE storage.buckets
         SET lifecycle_configuration = $2::jsonb,
             lifecycle_configuration_generation = $3::uuid
         WHERE id = $1`,
        [
          bucketId,
          JSON.stringify({
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
          }),
          generation,
        ]
      )
    })
    return generation
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
  })

  test('constructs, lists, claims, and revalidates only Standard-bucket shard state', async () => {
    const generation = await putConfiguration()
    const state = await tHelper.database.createNoncurrentLifecycleState(
      bucketId,
      generation,
      '2000-01-01T00:00:00.000Z'
    )
    expect(state).toMatchObject({
      bucketId,
      scanKind: 'NONCURRENT',
      shardId: 0,
      shardEpoch: '1',
      shardCount: 1,
    })

    await tHelper.database.connection.query(
      `INSERT INTO storage.bucket_lifecycle_states (
         bucket_id, scan_kind, shard_id, shard_epoch, shard_count,
         configuration_generation, next_run_at
       ) VALUES ($1, 'NONCURRENT', 0, 1, 1, $2, now())`,
      [analyticsBucketId, randomUUID()]
    )

    const due = await tHelper.database.listDueLifecycleShards(100)
    expect(due).toContainEqual({
      bucketId,
      scanKind: 'NONCURRENT',
      shardEpoch: '1',
      shardId: 0,
    })
    expect(due.some((coordinate) => coordinate.bucketId === analyticsBucketId)).toBe(false)

    const claimId = randomUUID()
    await expect(
      tHelper.database.claimLifecycleShard({
        bucketId: analyticsBucketId,
        scanKind: 'NONCURRENT',
        shardEpoch: '1',
        shardId: 0,
        claimId: randomUUID(),
        leaseMs: 60_000,
      })
    ).resolves.toBeUndefined()

    await expect(
      tHelper.database.claimLifecycleShard({
        bucketId,
        scanKind: 'NONCURRENT',
        shardEpoch: '1',
        shardId: 0,
        claimId,
        leaseMs: 60_000,
      })
    ).resolves.toMatchObject({ claimId })

    await expect(
      tHelper.database.revalidateLifecycleShardClaim({
        bucketId,
        scanKind: 'NONCURRENT',
        shardEpoch: '1',
        shardId: 0,
        claimId,
      })
    ).resolves.toMatchObject({ claimId })

    await expect(
      tHelper.database.revalidateLifecycleShardClaim({
        bucketId,
        scanKind: 'NONCURRENT',
        shardEpoch: '1',
        shardId: 0,
        claimId: randomUUID(),
      })
    ).resolves.toBeUndefined()
  })

  test.each([
    Number.MAX_SAFE_INTEGER,
    800_000,
    2_460_000,
  ])('evaluates a policy with %i days without timestamp overflow', async (noncurrentDays) => {
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
    const transaction = await tHelper.database.connection.transaction()
    try {
      for (const [name, archivedAt] of [
        ['old-a', '2026-07-01T00:00:00.000Z'],
        ['old-b', '2026-07-02T00:00:00.000Z'],
        ['young', '2026-08-10T00:00:00.000Z'],
      ]) {
        await transaction.query(
          `INSERT INTO storage.objects (
             id, bucket_id, name, version, archived_at, is_versioned, metadata
           ) VALUES ($1, $2, $3, $4, $5, true, '{"size": 10}'::jsonb)`,
          [randomUUID(), bucketId, name, randomUUID(), archivedAt]
        )
      }
      await transaction.commit()
    } catch (error) {
      await transaction.rollback()
      throw error
    }

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

  test('preserves durable progress when releasing after an ambiguous write', async () => {
    const generation = await putConfiguration()
    await tHelper.database.createNoncurrentLifecycleState(
      bucketId,
      generation,
      '2000-01-01T00:00:00.000Z'
    )
    const identity = {
      bucketId,
      scanKind: 'NONCURRENT' as const,
      shardEpoch: '1',
      shardId: 0,
      claimId: randomUUID(),
    }
    await tHelper.database.claimLifecycleShard({ ...identity, leaseMs: 60_000 })
    const durable = createLifecycleContinuation({
      runId: randomUUID(),
      trigger: 'scheduled',
      generation,
      snapshotAt: new Date().toISOString(),
      topology: {
        scanKind: 'NONCURRENT',
        epoch: '1',
        shardId: 0,
        shardCount: 1,
      },
    })
    durable.counters.versionsExamined = 2
    durable.counters.versionsEligible = 1
    durable.counters.objectVersionsDeleted = 1
    durable.counters.bytesDeleted = 10
    durable.counters.batchesCompleted = 1
    await tHelper.database.saveLifecycleContinuation(identity, durable)

    const nextRunAt = '2099-01-01T00:00:00.000Z'
    await expect(
      tHelper.database.releaseLifecycleShardClaim({
        ...identity,
        nextRunAt,
        error: { message: 'commit acknowledgement lost' },
      })
    ).resolves.toBe(true)
    const result = await tHelper.database.connection.query(
      `SELECT continuation, claim_id, claim_until, next_run_at, failure_count, last_error
       FROM storage.bucket_lifecycle_states WHERE bucket_id = $1`,
      [bucketId]
    )
    expect(result.rows).toEqual([
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
    const insertArchivedVersion = async (version: string, archivedAt: string) => {
      const transaction = await tHelper.database.connection.transaction()
      try {
        await transaction.query(
          `INSERT INTO storage.objects (
             id, bucket_id, name, version, archived_at, is_versioned, metadata
           ) VALUES ($1, $2, $3, $4, $5, true, '{"size": 10}'::jsonb)`,
          [randomUUID(), bucketId, name, version, archivedAt]
        )
        await transaction.commit()
      } catch (error) {
        await transaction.rollback()
        throw error
      }
    }

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
    const transaction = await tHelper.database.connection.transaction()
    try {
      for (let index = 0; index < 17; index++) {
        const name = `history/page-${index.toString().padStart(2, '0')}`
        for (const archivedAt of [
          '2026-06-01T00:00:00.000Z',
          '2026-06-02T00:00:00.000Z',
        ] as const) {
          await transaction.query(
            `INSERT INTO storage.objects (
                 id, bucket_id, name, version, archived_at, is_versioned, metadata
               ) VALUES ($1, $2, $3, $4, $5, true, '{"size": 10}'::jsonb)`,
            [randomUUID(), bucketId, name, randomUUID(), archivedAt]
          )
        }
      }
      await transaction.commit()
    } catch (error) {
      await transaction.rollback()
      throw error
    }

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
      await fixtures.setStatus(bucketId, 'ENABLED')
      if (options.suspended) {
        await fixtures.setStatus(bucketId, 'SUSPENDED')
      }
      const generation = await putConfiguration(1)
      await tHelper.database.createNoncurrentLifecycleState(
        bucketId,
        generation,
        '2000-01-01T00:00:00.000Z'
      )
      const name = 'history/retention-count-race'
      const olderVersion = options.legacy ? null : randomUUID()
      const newerVersion = randomUUID()
      const writtenAt = '2000-01-01T00:00:00.000Z'
      // These rows are explicit history fixtures. No writer or invalidation hook runs.
      await withServiceOperation('storage.object.upload', async (db) => {
        await db.query(
          `INSERT INTO storage.objects (
               id, bucket_id, name, version, created_at, archived_at, is_versioned, metadata, is_delete_marker
             ) VALUES
               ($1, $2, $3, $4, $6::timestamptz, '2001-01-01', $8, $7::jsonb, $9),
               (gen_random_uuid(), $2, $3, $5, $6::timestamptz, '2001-02-01', true, $10::jsonb, false)`,
          [
            randomUUID(),
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
      const identity = {
        bucketId,
        scanKind: 'NONCURRENT' as const,
        shardEpoch: '1',
        shardId: 0,
        claimId: randomUUID(),
      }
      await tHelper.database.claimLifecycleShard({ ...identity, leaseMs: 60_000 })
      const initial = createLifecycleContinuation({
        runId: randomUUID(),
        trigger: 'scheduled',
        generation,
        snapshotAt: '2001-06-01T00:00:00.000Z',
        topology: {
          scanKind: 'NONCURRENT',
          epoch: '1',
          shardId: 0,
          shardCount: 1,
        },
      })
      await tHelper.database.saveLifecycleContinuation(identity, initial)
      const evaluation = {
        bucketId,
        snapshotAt: initial.snapshotAt,
        rules: [{ cutoffAt: '2001-05-01T00:00:00.000Z', newerNoncurrentVersions: 1 }],
        pageSize: 10,
      }
      const page = await tHelper.database.evaluateNoncurrentLifecyclePage(evaluation)
      expect(page.candidates).toEqual([
        { name, version: olderVersion, isDeleteMarker: options.marker ?? false },
      ])
      const staged = stageLifecycleBatch(initial, {
        pageEnd: page.pageEnd!,
        rawRowsExamined: page.rawRowsExamined,
        versions: page.candidates.map(freezeLifecycleBatchVersion),
      })
      const armed = armLifecycleAttempt(staged, {
        attemptId: randomUUID(),
        authorizedGeneration: generation,
        startedAt: new Date().toISOString(),
      })
      const arm = () =>
        tHelper.database.armLifecycleAttempt({
          ...identity,
          configurationGeneration: generation,
          leaseMs: 60_000,
          continuation: armed,
        })
      return {
        identity,
        staged,
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
      await tHelper.database.saveLifecycleContinuation(fixture.identity, fixture.staged)
      await expect(fixture.arm()).resolves.toMatchObject({ continuation: fixture.armed })
    })

    test('rechecks retention at arm time after a raw fixture history change', async () => {
      const fixture = await stageCountProtectedVersion()
      await tHelper.database.saveLifecycleContinuation(fixture.identity, fixture.staged)
      await withServiceOperation('storage.lifecycle.test_fixture', async (transaction) => {
        await transaction.query(`SET LOCAL storage.allow_delete_query = 'true'`)
        await transaction.query(
          'DELETE FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
          [bucketId, fixture.name, fixture.newerVersion]
        )
      })
      // SQL fixture mutation supplies no invalidation hook. The native arm path
      // must independently discover that the retained-sibling count no longer holds.
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
})
