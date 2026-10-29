import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import {
  buildEvaluateNoncurrentLifecyclePageStatement,
  mapLifecycleEvaluationPage,
} from '@storage/database/lifecycle'
import { compileLifecycleEvaluationRules } from '@storage/lifecycle/configuration'
import {
  armLifecycleAttempt,
  encodeLifecycleContinuation,
  recordLifecycleArtifactOutcomes,
} from '@storage/lifecycle/continuation'
import { lifecycleEvaluationBatches } from '@storage/lifecycle/evaluation'
import type { NoncurrentLifecycleCursor } from '@storage/schemas'
import { Client } from 'pg'
import { getConfig } from '../config'
import { armedContinuation, stagedContinuation } from './utils/lifecycle-continuation'
import { useLifecycleStorage } from './utils/lifecycle-versioning'
import { withDeleteEnabled } from './utils/storage'

describe('noncurrent lifecycle database API', () => {
  const fixtures = useLifecycleStorage('lifecycle-db')
  const tHelper = fixtures.helper
  const { withServiceOperation } = fixtures
  let bucketId: string
  beforeEach(() => {
    bucketId = fixtures.bucketId
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

  async function insertOtherBucketSibling(name: string, archivedAt: string) {
    const otherBucketId = await fixtures.createBucket()
    await tHelper.database.connection.query(
      `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned, metadata)
       VALUES ($1, $2, gen_random_uuid()::text, $3::timestamptz, true, '{"size": 10}'::jsonb)`,
      [otherBucketId, name, archivedAt]
    )
  }

  async function putConfiguration(
    newerNoncurrentVersions?: number,
    beforeWrite?: () => Promise<void>
  ) {
    await fixtures.setStatus(bucketId, 'ENABLED')
    await beforeWrite?.()
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

  const SNAPSHOT_AT = '2026-08-15T12:00:00.000Z'

  const evaluatePage = (
    rules: Array<{ cutoffAt: string; newerNoncurrentVersions?: number }>,
    extra: { pageSize?: number; cursor?: NoncurrentLifecycleCursor } = {}
  ) =>
    tHelper.database.evaluateNoncurrentLifecyclePage({
      bucketId,
      snapshotAt: SNAPSHOT_AT,
      pageSize: 480,
      rules,
      ...extra,
    })

  const configuration = (noncurrentDays: number) => ({
    rules: [
      {
        status: 'Enabled' as const,
        filter: {},
        noncurrentVersionExpiration: { noncurrentDays },
      },
    ],
  })
  const dueNow = async () =>
    (await fixtures.readState(bucketId, 'next_run_at <= clock_timestamp() AS due'))[0].due

  test('evaluates a sparse page once and preserves each chunk boundary and the ineligible tail', async () => {
    const archivedAt = '2026-07-01T00:00:00.000123Z'
    await insertArchivedObjects([
      ...['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'].map((name) => ({ name, archivedAt })),
      ...['b', 'd', 'f'].map((name) => ({ name, archivedAt: '2026-08-10T00:00:00Z' })),
    ])
    const rules = [{ cutoffAt: '2026-08-01T00:00:00Z', newerNoncurrentVersions: 1 }]
    const page = await evaluatePage(rules, { pageSize: 100 })
    expect(page.candidates.map(({ name, rawRowNumber }) => ({ name, rawRowNumber }))).toEqual([
      { name: 'b', rawRowNumber: 2 },
      { name: 'd', rawRowNumber: 4 },
      { name: 'f', rawRowNumber: 6 },
    ])
    expect(page).toMatchObject({
      rawRowsExamined: 8,
      pageEnd: { name: 'h', archivedAt },
      exhausted: true,
    })
    expect([...lifecycleEvaluationBatches(page, 2)]).toMatchObject([
      {
        rawRowsExamined: 4,
        pageEnd: { name: 'd', archivedAt },
        candidates: [{ name: 'b' }, { name: 'd' }],
      },
      { rawRowsExamined: 4, pageEnd: { name: 'h', archivedAt }, candidates: [{ name: 'f' }] },
    ])
    const empty = await evaluatePage(rules, {
      pageSize: 100,
      cursor: page.pageEnd,
    })
    expect(empty).toEqual({ rawRowsExamined: 0, candidates: [], exhausted: true })
  })

  test('loads typed Standard-bucket authority', async () => {
    const analyticsBucketId = await fixtures.createBucket('ANALYTICS')
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

  test('lists, claims, and revalidates only Standard-bucket state', async () => {
    const analyticsBucketId = await fixtures.createBucket('ANALYTICS')
    await putConfiguration()

    await tHelper.database.connection.query(
      `INSERT INTO storage.bucket_lifecycle_states (
         bucket_id, next_run_at
       ) VALUES ($1, now())`,
      [analyticsBucketId]
    )

    const due = await tHelper.database.listDueLifecycleBuckets(100)
    expect(due).toContainEqual(bucketId)
    expect(due.some((id) => id === analyticsBucketId)).toBe(false)

    const claimId = randomUUID()
    await expect(
      tHelper.database.claimLifecycleBucket({
        bucketId: analyticsBucketId,
        claimId: randomUUID(),
        leaseMs: 60_000,
      })
    ).resolves.toBeUndefined()

    await expect(
      tHelper.database.claimLifecycleBucket({ bucketId, claimId, leaseMs: 60_000 })
    ).resolves.toMatchObject({ claimId })

    await expect(
      tHelper.database.revalidateLifecycleClaim({ bucketId, claimId })
    ).resolves.toMatchObject({ claimId })

    await expect(
      tHelper.database.revalidateLifecycleClaim({ bucketId, claimId: randomUUID() })
    ).resolves.toBeUndefined()
  })

  test.each([Number.MAX_SAFE_INTEGER, 800_000, 2_460_000])(
    'evaluates a policy with %i days without timestamp overflow',
    async (noncurrentDays) => {
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
    }
  )

  test('evaluates all rules in the database while raw-page progress stays selectivity-independent', async () => {
    await insertArchivedObjects([
      { name: 'old-a', archivedAt: '2026-07-01T00:00:00.000Z' },
      { name: 'old-b', archivedAt: '2026-07-02T00:00:00.000Z' },
      { name: 'young', archivedAt: '2026-08-10T00:00:00.000Z' },
    ])

    const page = await evaluatePage([
      { cutoffAt: '2026-08-01T00:00:00.000Z' },
      { cutoffAt: '2026-07-15T00:00:00.000Z' },
    ])

    expect(page.rawRowsExamined).toBe(2)
    expect(page.exhausted).toBe(true)
    expect(page.pageEnd).toBeDefined()
    expect(page.candidates.map((candidate) => candidate.name).sort()).toEqual(['old-a', 'old-b'])
    expect(new Set(page.candidates.map((candidate) => candidate.version)).size).toBe(2)
  })

  test('excludes rows archived exactly at each rule cutoff', async () => {
    await insertArchivedObjects([
      { name: 'at-first-cutoff', archivedAt: '2026-07-15T00:00:00.000Z' },
      { name: 'at-latest-cutoff', archivedAt: '2026-08-01T00:00:00.000Z' },
    ])

    await expect(
      evaluatePage([
        { cutoffAt: '2026-07-15T00:00:00.000Z' },
        { cutoffAt: '2026-08-01T00:00:00.000Z', newerNoncurrentVersions: 1 },
      ])
    ).resolves.toMatchObject({ rawRowsExamined: 1, candidates: [] })
  })

  test('skips an archived row with an empty version instead of failing evaluation', async () => {
    await insertArchivedObjects([
      { name: 'blank-version', archivedAt: '2026-07-01T00:00:00.000Z', version: '' },
      { name: 'valid', archivedAt: '2026-07-02T00:00:00.000Z' },
    ])

    const page = await evaluatePage([{ cutoffAt: '2026-08-01T00:00:00.000Z' }])

    expect(page.rawRowsExamined).toBe(2)
    expect(page.exhausted).toBe(true)
    expect(page.candidates.map((candidate) => candidate.name)).toEqual(['valid'])
  })

  test.each([
    { label: 'an empty name', name: '', archivedAt: '2026-07-01T00:00:00.000Z' },
    { label: 'a NULL name', name: null, archivedAt: '2026-07-01T00:00:00.000Z' },
    { label: 'a BC archived_at', name: 'poison', archivedAt: '0044-03-15T00:00:00Z BC' },
    { label: 'a -infinity archived_at', name: 'poison', archivedAt: '-infinity' },
  ])('skips an archived row with $label instead of failing evaluation', async (poison) => {
    await insertArchivedObjects([
      { name: poison.name as string, archivedAt: poison.archivedAt },
      { name: 'valid', archivedAt: '2026-07-02T00:00:00.000Z' },
    ])

    const page = await evaluatePage([{ cutoffAt: '2026-08-01T00:00:00.000Z' }])

    expect(page.rawRowsExamined).toBe(1)
    expect(page.exhausted).toBe(true)
    expect(page.candidates.map((candidate) => candidate.name)).toEqual(['valid'])
    expect(page.pageEnd?.name).toBe('valid')
  })

  test('renders the page cursor in UTC under a session zone with a seconds offset', async () => {
    await insertArchivedObjects([{ name: 'old', archivedAt: '1970-06-01T00:00:00Z' }])
    const statement = buildEvaluateNoncurrentLifecyclePageStatement({
      bucketId,
      snapshotAt: SNAPSHOT_AT,
      pageSize: 480,
      rules: [{ cutoffAt: '2026-08-01T00:00:00.000Z' }],
    })

    const client = new Client({ connectionString: getConfig().databaseURL })
    await client.connect()
    try {
      await client.query("SET TIME ZONE 'Africa/Monrovia'")
      const result = await client.query(statement)
      const page = mapLifecycleEvaluationPage(result.rows[0], 480)
      expect(page.pageEnd).toEqual({ name: 'old', archivedAt: '1970-06-01T00:00:00.000000Z' })
    } finally {
      await client.end()
    }
  })

  test('reconsiders an age-due row when a newer sibling later satisfies the retention count', async () => {
    const name = 'history/count-protected'
    const versions = [randomUUID(), randomUUID(), randomUUID()]
    const insertArchivedVersion = (version: string, archivedAt: string) =>
      insertArchivedObjects([{ name, version, archivedAt }])

    await insertArchivedVersion(versions[0], '2026-06-01T00:00:00.000Z')
    await insertArchivedVersion(versions[1], '2026-06-02T00:00:00.000Z')

    const evaluateFromRangeStart = () =>
      evaluatePage([{ cutoffAt: '2026-08-01T00:00:00.000Z', newerNoncurrentVersions: 2 }])

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
    { label: 'in another bucket', sibling: 'other' },
    { label: 'archived after the snapshot', sibling: 'late' },
  ])('does not count a newer sibling $label', async ({ sibling }) => {
    const name = 'history/count-scope'
    await insertArchivedObjects([
      { name, archivedAt: '2026-06-01T00:00:00.000Z' },
      ...(sibling === 'late' ? [{ name, archivedAt: '2026-09-01T00:00:00.000Z' }] : []),
    ])
    if (sibling === 'other') await insertOtherBucketSibling(name, '2026-07-01T00:00:00.000Z')

    await expect(
      evaluatePage([{ cutoffAt: '2026-08-01T00:00:00.000Z', newerNoncurrentVersions: 1 }])
    ).resolves.toMatchObject({ candidates: [] })
  })

  test.each([1, 17, 480])(
    'produces the same candidates and counters with page size %i',
    async (pageSize) => {
      await insertArchivedObjects(
        Array.from({ length: 17 }, (_, index) =>
          ['2026-06-01T00:00:00.000Z', '2026-06-02T00:00:00.000Z'].map((archivedAt) => ({
            name: `history/page-${index.toString().padStart(2, '0')}`,
            archivedAt,
          }))
        ).flat()
      )

      let cursor: NoncurrentLifecycleCursor | undefined
      let rawRowsExamined = 0
      const candidates: string[] = []
      for (;;) {
        const page = await evaluatePage(
          [{ cutoffAt: '2026-08-01T00:00:00.000Z', newerNoncurrentVersions: 1 }],
          { pageSize, cursor }
        )
        rawRowsExamined += page.rawRowsExamined
        candidates.push(...page.candidates.map((candidate) => candidate.name))
        if (page.exhausted) break
        cursor = page.pageEnd
      }

      expect(rawRowsExamined).toBe(34)
      expect(candidates).toEqual(
        [...Array(17).keys()].map((index) => `history/page-${index.toString().padStart(2, '0')}`)
      )
    }
  )

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
      const identity = { bucketId, claimId: randomUUID() }
      await tHelper.database.claimLifecycleBucket({ ...identity, leaseMs: 60_000 })
      const snapshotAt = '2001-06-01T00:00:00.000Z'
      const evaluation = {
        bucketId,
        snapshotAt,
        rules: [{ cutoffAt: '2001-05-01T00:00:00.000Z', newerNoncurrentVersions: 1 }],
        pageSize: 10,
      }
      const page = await tHelper.database.evaluateNoncurrentLifecyclePage(evaluation)
      expect(page.candidates).toEqual([
        expect.objectContaining({
          id: olderId,
          name,
          version: olderVersion,
          isDeleteMarker: options.marker ?? false,
        }),
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
        olderId,
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
    ])(
      'arms $label using the compatible row identity and policy',
      async ({ label: _label, ...options }) => {
        const fixture = await stageCountProtectedVersion(options)
        await expect(fixture.arm()).resolves.toMatchObject({
          continuation: {
            batch: { inFlight: { attemptId: fixture.armed.batch!.inFlight!.attemptId } },
          },
        })
        await expect(
          tHelper.database.connection.query('SELECT 1 FROM storage.objects WHERE id = $1', [
            fixture.olderId,
          ])
        ).resolves.toMatchObject({ rowCount: 0 })
      }
    )

    test('rechecks retention at arm time after a raw fixture history change', async () => {
      const fixture = await stageCountProtectedVersion()
      await withServiceOperation('storage.lifecycle.test_fixture', async (transaction) => {
        await transaction.query(
          'DELETE FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
          [bucketId, fixture.name, fixture.newerVersion]
        )
      })
      await expect(fixture.arm()).resolves.toBeUndefined()
      await expect(
        tHelper.database.revalidateLifecycleClaim(fixture.identity)
      ).resolves.toBeUndefined()
      await expect(
        tHelper.database.claimLifecycleBucket({
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
  it('records artifact outcomes and advances only once every artifact is resolved', async () => {
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
    const claimed = await tHelper.database.claimLifecycleBucket({
      bucketId,
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
      bucketId,
      claimId,
      leaseMs: 120_000,
      continuation: armed,
    })
    expect(armedState?.continuation).toMatchObject({
      batch: { inFlight: { attemptId: armed.batch!.inFlight!.attemptId } },
    })

    const partiallyRecorded = recordLifecycleArtifactOutcomes(
      { ...armed, counters: armedState!.continuation!.counters },
      new Map<string, { outcome: 'DELETED' | 'UNRESOLVED'; error?: string }>([
        [
          armed.batch!.versions[0].artifacts[0].key,
          { outcome: 'UNRESOLVED' as const, error: 'retryable backend failure' },
        ],
        [armed.batch!.versions[0].artifacts[1].key, { outcome: 'DELETED' as const }],
        ...armed.batch!.versions[1].artifacts.map(
          (artifact) => [artifact.key, { outcome: 'DELETED' as const }] as const
        ),
      ])
    )
    const partial = await tHelper.database.commitLifecycleAttempt({
      bucketId,
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
              expect.objectContaining({ outcome: 'UNRESOLVED' }),
              expect.objectContaining({ outcome: 'DELETED' }),
            ],
          },
        ],
      },
      counters: { objectVersionsDeleted: 2, bytesDeleted: 50, batchesCompleted: 0 },
    })

    const recorded = recordLifecycleArtifactOutcomes(
      partial!.continuation!,
      new Map([[armed.batch!.versions[0].artifacts[0].key, { outcome: 'DELETED' as const }]])
    )
    const committed = await tHelper.database.commitLifecycleAttempt({
      bucketId,
      claimId,
      attemptId: armed.batch!.inFlight!.attemptId,
      continuation: recorded,
    })
    expect(committed?.continuation).toMatchObject({
      cursor: { name: completedName, archivedAt: candidate.archivedAt },
      counters: { objectVersionsDeleted: 2, bytesDeleted: 50, batchesCompleted: 1 },
    })
  })
  describe('bucket queries', () => {
    const claim = (id = bucketId, leaseMs = 60_000) =>
      tHelper.database.claimLifecycleBucket({ bucketId: id, claimId: randomUUID(), leaseMs })
    const setState = (sets: string, id = bucketId, values: unknown[] = []) =>
      tHelper.database.connection.query(
        `UPDATE storage.bucket_lifecycle_states SET ${sets} WHERE bucket_id = $1`,
        [id, ...values]
      )
    const dispatchState = (options: { inFlightOnly?: boolean } = {}) =>
      tHelper.database.findLifecycleDispatchState({ runBudgetMs: 20_000, ...options })
    const listed = async (id: string) =>
      (await tHelper.database.listDueLifecycleBuckets(1000)).some((bucketId) => bucketId === id)

    test('excludes a live claim from the due list and from the next dispatch time', async () => {
      await putConfiguration()
      expect(await listed(bucketId)).toBe(true)
      await expect(claim()).resolves.toBeDefined()

      expect(await listed(bucketId)).toBe(false)
      const { nextDispatchAt: next } = await dispatchState()
      expect(next!.getTime()).toBeGreaterThan(Date.now())

      await setState("claim_until = now() - interval '1 second'")
      expect(await listed(bucketId)).toBe(true)
    })

    test.each([
      {
        name: 'the run budget when it ends before the lease',
        leaseMs: 3_600_000,
        expectedMs: 20_000,
      },
      {
        name: 'the lease expiry when it ends before the run budget',
        leaseMs: 5_000,
        expectedMs: 5_000,
      },
    ])('reports a claimed bucket at $name rather than as due', async ({ leaseMs, expectedMs }) => {
      await putConfiguration()
      await setState("next_run_at = '2000-01-01'")
      const startedAt = new Date()
      await setState(
        `claim_id = gen_random_uuid(), last_started_at = $2, claim_until = $3`,
        bucketId,
        [startedAt, new Date(startedAt.getTime() + leaseMs)]
      )

      const { nextDispatchAt: next } = await dispatchState()
      expect(next).toEqual(new Date(startedAt.getTime() + expectedMs))
    })

    test('cannot claim a bucket that is not yet due', async () => {
      await putConfiguration()
      await setState("next_run_at = now() + interval '1 hour'")
      expect(await listed(bucketId)).toBe(false)
      await expect(claim()).resolves.toBeUndefined()
    })

    test('stops revalidating a claim once its lease has passed', async () => {
      await putConfiguration()
      const claimId = randomUUID()
      await tHelper.database.claimLifecycleBucket({ bucketId, claimId, leaseMs: 60_000 })
      await expect(
        tHelper.database.revalidateLifecycleClaim({ bucketId, claimId })
      ).resolves.toMatchObject({ claimId })

      await setState("claim_until = now() - interval '1 second'")
      await expect(
        tHelper.database.revalidateLifecycleClaim({ bucketId, claimId })
      ).resolves.toBeUndefined()
    })

    test('release keeps the last error on a clean release and stamps completion', async () => {
      await putConfiguration()
      const claimId = randomUUID()
      await tHelper.database.claimLifecycleBucket({ bucketId, claimId, leaseMs: 60_000 })
      await tHelper.database.releaseLifecycleClaim({
        bucketId,
        claimId,
        nextRunAt: new Date().toISOString(),
        error: { message: 'boom' },
      })
      await expect(
        fixtures.readState(bucketId, 'failure_count, last_error, last_completed_at')
      ).resolves.toMatchObject([
        { failure_count: 1, last_error: { message: 'boom' }, last_completed_at: expect.any(Date) },
      ])

      const second = randomUUID()
      await expect(
        tHelper.database.claimLifecycleBucket({ bucketId, claimId: second, leaseMs: 60_000 })
      ).resolves.toBeDefined()
      await setState('last_completed_at = NULL')
      await tHelper.database.releaseLifecycleClaim({
        bucketId,
        claimId: second,
        nextRunAt: new Date().toISOString(),
      })
      await expect(
        fixtures.readState(bucketId, 'failure_count, last_error, last_completed_at')
      ).resolves.toMatchObject([
        { failure_count: 0, last_error: { message: 'boom' }, last_completed_at: expect.any(Date) },
      ])
    })

    test('completes a run only for the claim that holds the bucket', async () => {
      const generation = await putConfiguration()
      const claimId = randomUUID()
      await tHelper.database.claimLifecycleBucket({ bucketId, claimId, leaseMs: 60_000 })
      await setState(`continuation = '{"continuationVersion": 1}'::jsonb`)

      await expect(
        tHelper.database.completeLifecycleRun(
          { bucketId, claimId: randomUUID(), generation },
          {},
          new Date().toISOString()
        )
      ).resolves.toBe(false)
      await expect(fixtures.readState(bucketId, 'continuation, claim_id')).resolves.toEqual([
        { continuation: { continuationVersion: 1 }, claim_id: claimId },
      ])
    })

    test.each(['omitted', 'different'] as const)(
      'uses the run generation even when the result generation is %s',
      async (resultGeneration) => {
        const generation = await putConfiguration()
        const claimId = randomUUID()
        await expect(
          tHelper.database.claimLifecycleBucket({ bucketId, claimId, leaseMs: 60_000 })
        ).resolves.toBeDefined()
        const lastResult = resultGeneration === 'omitted' ? {} : { generation: randomUUID() }
        const nextRunAt = '2099-01-01T00:00:00.000Z'

        await expect(
          tHelper.database.completeLifecycleRun(
            { bucketId, claimId, generation },
            lastResult,
            nextRunAt
          )
        ).resolves.toBe(true)
        await expect(fixtures.readState(bucketId, 'next_run_at, last_result')).resolves.toEqual([
          { next_run_at: new Date(nextRunAt), last_result: lastResult },
        ])
        await expect(claim()).resolves.toBeUndefined()
      }
    )

    test('rejects completion without a run generation before changing state', async () => {
      await putConfiguration()
      const claimId = randomUUID()
      await expect(
        tHelper.database.claimLifecycleBucket({ bucketId, claimId, leaseMs: 60_000 })
      ).resolves.toBeDefined()
      await expect(
        tHelper.database.completeLifecycleRun(
          { bucketId, claimId, generation: undefined as unknown as string },
          {},
          '2099-01-01T00:00:00.000Z'
        )
      ).rejects.toMatchObject({ code: 'InvalidParameter' })
      await expect(fixtures.readState(bucketId, 'claim_id, last_result')).resolves.toEqual([
        { claim_id: claimId, last_result: null },
      ])
    })

    test('keeps a policy written under a racing claim due after the claimed run completes', async () => {
      await putConfiguration()
      await setState(`next_run_at = '2000-01-01'`)
      const stale = (await tHelper.database.findLifecycleBucket(bucketId))
        .lifecycle_configuration_generation!
      const writer = new Client({ connectionString: getConfig().databaseURL })
      await writer.connect()
      try {
        await writer.query('BEGIN')
        const pid = (await writer.query('SELECT pg_backend_pid() AS pid')).rows[0].pid
        await writer.query('SET LOCAL ROLE service_role')
        await writer.query(
          `UPDATE storage.buckets SET lifecycle_configuration = $2::jsonb,
             lifecycle_configuration_generation = gen_random_uuid() WHERE id = $1`,
          [bucketId, JSON.stringify(configuration(1))]
        )
        const identity = { bucketId, claimId: randomUUID() }
        const claim = tHelper.database.claimLifecycleBucket({ ...identity, leaseMs: 60_000 })
        await expect
          .poll(
            async () =>
              (
                await tHelper.database.connection.query(
                  'SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))) AS blocked',
                  [pid]
                )
              ).rows[0].blocked
          )
          .toBe(true)
        await writer.query('COMMIT')
        expect((await claim)?.bucket.lifecycle_configuration_generation).toBe(stale)

        await expect(
          tHelper.database.completeLifecycleRun(
            { ...identity, generation: stale },
            {},
            new Date(Date.now() + 86_400_000).toISOString()
          )
        ).resolves.toBe(true)
        await expect(dueNow()).resolves.toBe(true)
      } finally {
        await writer.end()
      }
    })

    test('does not report a claimed bucket that has no scheduled run', async () => {
      await putConfiguration()
      await expect(claim()).resolves.toBeDefined()
      await setState('next_run_at = NULL')

      await expect(dispatchState()).resolves.toMatchObject({ nextDispatchAt: null })
    })

    test('restricts due buckets and the next dispatch time to journals in flight on request', async () => {
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

      const all = await tHelper.database.listDueLifecycleBuckets(1000)
      expect(all).toEqual(expect.arrayContaining([bucketId, other]))
      expect(all.indexOf(other)).toBeLessThan(all.indexOf(bucketId))
      const filtered = await tHelper.database.listDueLifecycleBuckets(1000, { inFlightOnly: true })
      expect(filtered).toContain(bucketId)
      expect(filtered).not.toContain(other)

      const { nextDispatchAt: next } = await dispatchState({ inFlightOnly: true })
      expect(next).toEqual(new Date('2000-01-02'))
      await expect(dispatchState()).resolves.toMatchObject({
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
      tHelper.database.findLifecycleDispatchState({ inFlightOnly: true, runBudgetMs: 20_000 })
    ).resolves.toEqual({
      nextDispatchAt: new Date('2026-01-01'),
      maxFailureCount: 9,
      oldestInFlightStartedAt: '2025-12-01T00:00:00.000Z',
      hasAbandonedArtifacts: false,
    })
    await tHelper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states SET continuation = jsonb_set(continuation, '{batch,inFlight,startedAt}', '"invalid"') WHERE bucket_id = $1`,
      [other]
    )
    await expect(
      tHelper.database.findLifecycleDispatchState({ runBudgetMs: 20_000 })
    ).resolves.toMatchObject({
      nextDispatchAt: new Date('2026-01-01'),
      maxFailureCount: 9,
      oldestInFlightStartedAt: null,
    })
  })

  describe('arm and commit coordination', () => {
    async function armBatch(
      names: string[],
      options: {
        archivedAt?: string
        snapshotAt?: string
        newerNoncurrentVersions?: number
        beforeConfiguration?: () => Promise<void>
        versions?: string[]
      } = {}
    ) {
      const rows = names.map((name, index) => ({
        id: randomUUID(),
        name,
        version: options.versions?.[index] ?? randomUUID(),
        archivedAt:
          options.archivedAt ??
          (options.versions ? `2026-06-01T00:00:00.00${index}Z` : '2026-06-01T00:00:00.000Z'),
      }))
      await insertArchivedObjects(rows)
      const generation = await putConfiguration(
        options.newerNoncurrentVersions,
        options.beforeConfiguration
      )
      const identity = { bucketId, claimId: randomUUID() }
      await tHelper.database.claimLifecycleBucket({ ...identity, leaseMs: 60_000 })
      const staged = stagedContinuation(
        generation,
        rows.map(({ id, name, version }) => ({ id, name, version, isDeleteMarker: false })),
        { name: rows.at(-1)!.name, archivedAt: rows.at(-1)!.archivedAt },
        options
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

    test('rejects oversized new arms before deleting metadata or persisting a journal', async () => {
      const fixture = await armBatch(Array.from({ length: 49 }, (_, index) => `oversized/${index}`))

      await expect(fixture.arm()).rejects.toMatchObject({ code: 'InvalidParameter' })
      const remaining = await tHelper.database.connection.query(
        'SELECT count(*)::integer AS count FROM storage.objects WHERE bucket_id = $1',
        [bucketId]
      )
      expect(remaining.rows).toEqual([{ count: 49 }])
      await expect(fixtures.readState(bucketId, 'continuation')).resolves.toEqual([
        { continuation: null },
      ])
    })

    test('rejects arm and commit inputs whose journal does not describe an armed attempt', async () => {
      const fixture = await armBatch(['invalid-input'])

      await expect(
        tHelper.database.armLifecycleAttempt({ ...fixture.input, continuation: fixture.staged })
      ).rejects.toMatchObject({ code: 'InvalidParameter' })
      await expect(
        tHelper.database.commitLifecycleAttempt({
          ...fixture.identity,
          attemptId: randomUUID(),
          continuation: fixture.recorded(),
        })
      ).rejects.toMatchObject({ code: 'InvalidParameter' })
    })

    test.each([
      { state: 'armed', capped: true },
      { state: 'unreadable armed', capped: true },
      { state: 'unreadable idle', capped: false },
      { state: 'empty', capped: false },
    ])('caps only preserved journals on release: $state', async ({ state, capped }) => {
      const fixture = await armBatch(['retry-cap'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const continuation =
        state === 'armed'
          ? encodeLifecycleContinuation(fixture.armed)
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
        tHelper.database.releaseLifecycleClaim({
          ...fixture.identity,
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

    test.each([
      { continuation: 'idle', error: false, expected: 0 },
      { continuation: 'armed', error: false, expected: 7 },
      { continuation: 'idle', error: true, expected: 8 },
    ])(
      'release with $continuation journal, error=$error leaves failure_count $expected',
      async ({ continuation, error, expected }) => {
        const fixture = await armBatch(['failure-count'])
        const armed = await fixture.arm()
        expect(armed).toBeDefined()
        if (continuation === 'idle') await expect(fixture.commit()).resolves.toBeDefined()
        await tHelper.database.connection.query(
          'UPDATE storage.bucket_lifecycle_states SET failure_count = 7 WHERE bucket_id = $1',
          [bucketId]
        )
        await expect(
          tHelper.database.releaseLifecycleClaim({
            ...fixture.identity,
            ...(continuation === 'idle' && { continuation: fixture.staged }),
            nextRunAt: '2099-01-01T01:00:00.000Z',
            ...(error && { error: { message: 'boom' } }),
          })
        ).resolves.toBe(true)
        await expect(fixtures.readState(bucketId, 'failure_count')).resolves.toEqual([
          { failure_count: expected },
        ])
      }
    )

    test.each([
      { journal: 'preserved', outcome: 'InvalidParameter' },
      { journal: 'armed', outcome: 'InvalidParameter' },
      { journal: 'staged', outcome: true },
      { journal: 'empty', outcome: true },
    ] as const)(
      'release without a schedule with $journal journal: $outcome',
      async ({ journal, outcome }) => {
        const fixture = await armBatch(['release/unscheduled'])
        await expect(fixture.arm()).resolves.toBeDefined()
        if (journal === 'staged' || journal === 'empty') {
          await expect(fixture.commit()).resolves.toBeDefined()
        }
        const continuation = {
          preserved: undefined,
          armed: fixture.armed,
          staged: fixture.staged,
          empty: null,
        }[journal]

        await expect(
          tHelper.database
            .releaseLifecycleClaim({ ...fixture.identity, continuation, nextRunAt: null })
            .catch((error: { code: string }) => error.code)
        ).resolves.toBe(outcome)
      }
    )

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

    test('rejects arming over a journal in flight even when its rows are eligible again', async () => {
      const fixture = await armBatch(['arm/rearmed'])
      await expect(fixture.arm()).resolves.toBeDefined()
      await insertArchivedObjects(fixture.rows)
      const before = await fixtures.readState(bucketId, 'continuation')

      await expect(fixture.arm()).resolves.toBeUndefined()
      await expect(fixtures.readState(bucketId, 'continuation')).resolves.toEqual(before)
    })

    test.each([
      {
        change: 'versioning is disabled',
        sql: "UPDATE storage.buckets SET versioning_status = 'DISABLED' WHERE id = $1",
        claimKept: true,
      },
      {
        change: 'the version became a delete marker',
        sql: 'UPDATE storage.objects SET is_delete_marker = true WHERE bucket_id = $1',
        claimKept: false,
      },
      {
        change: 'a policy replacement that keeps the row eligible',
        sql: `UPDATE storage.buckets SET lifecycle_configuration_generation = gen_random_uuid(),
                lifecycle_configuration = jsonb_set(lifecycle_configuration,
                  '{rules,0,noncurrentVersionExpiration,noncurrentDays}', '1')
              WHERE id = $1`,
        claimKept: true,
      },
    ])('does not arm a staged batch after $change', async ({ sql, claimKept }) => {
      const fixture = await armBatch(['arm/recheck'])
      const writer = await holdWriter(
        sql,
        [bucketId],
        'SET LOCAL session_replication_role = replica'
      )
      await writer.finish('COMMIT')

      await expect(fixture.arm()).resolves.toBeUndefined()
      await expect(fixtures.readState(bucketId, 'continuation, claim_id')).resolves.toEqual([
        { continuation: null, claim_id: claimKept ? fixture.identity.claimId : null },
      ])
    })

    test('does not arm a batch when only one of its rows lost eligibility', async () => {
      const fixture = await armBatch(['arm/partial-a', 'arm/partial-b'])
      const writer = await holdWriter(
        'UPDATE storage.objects SET is_delete_marker = true WHERE bucket_id = $1 AND name = $2',
        [bucketId, 'arm/partial-b'],
        'SET LOCAL session_replication_role = replica'
      )
      await writer.finish('COMMIT')

      await expect(fixture.arm()).resolves.toBeUndefined()
      await expect(fixtures.readState(bucketId, 'continuation, claim_id')).resolves.toEqual([
        { continuation: null, claim_id: null },
      ])
    })

    test('persists the armed journal without artifact keys', async () => {
      const fixture = await armBatch(['arm/no-keys'])
      await expect(fixture.arm()).resolves.toBeDefined()

      await expect(
        fixtures.readState(
          bucketId,
          "jsonb_path_exists(continuation, '$.batch.versions[*].artifacts[*].key') AS has_key, jsonb_array_length(continuation #> '{batch,versions,0,artifacts}') AS artifacts"
        )
      ).resolves.toEqual([{ has_key: false, artifacts: 2 }])
    })

    async function reference(id: string) {
      await tHelper.database.connection.query(
        'CREATE TABLE public.lifecycle_arm_refs (object_id uuid REFERENCES storage.objects(id))'
      )
      await tHelper.database.connection.query('INSERT INTO public.lifecycle_arm_refs VALUES ($1)', [
        id,
      ])
    }
    const dropReferences = () =>
      tHelper.database.connection.query('DROP TABLE IF EXISTS public.lifecycle_arm_refs')

    test('arms only the versions whose metadata delete is not vetoed', async () => {
      const fixture = await armBatch(['arm/vetoed', 'arm/kept'])
      try {
        await reference(fixture.rows[0].id)
        const state = await fixture.arm()

        expect(state?.claimId).toBe(fixture.identity.claimId)
        expect(state?.continuation?.batch?.versions.map(({ id }) => id)).toEqual([
          fixture.rows[1].id,
        ])
        expect(state?.continuation?.counters).toMatchObject({ objectVersionsDeleted: 1 })
        await expect(
          fixtures.readState(
            bucketId,
            "jsonb_path_exists(continuation, '$.batch.versions[*].artifacts[*].key') AS has_key"
          )
        ).resolves.toEqual([{ has_key: false }])
        expect(await rowExists(fixture.rows[0])).toBe(true)
        expect(await rowExists(fixture.rows[1])).toBe(false)
      } finally {
        await dropReferences()
      }
    })

    test('advances the cursor past a page whose every version is vetoed without arming', async () => {
      const fixture = await armBatch(['arm/all-vetoed'])
      const { batch: _batch, pageEnd, ...rest } = fixture.staged
      const skipped = { ...rest, cursor: pageEnd }
      try {
        await reference(fixture.rows[0].id)
        const state = await fixture.arm()

        expect(state?.claimId).toBe(fixture.identity.claimId)
        expect(state?.continuation).toEqual(skipped)
        await expect(
          fixtures.readState(
            bucketId,
            'continuation, claim_id, claim_until > now() AS claimed, next_run_at <= now() AS due'
          )
        ).resolves.toEqual([
          { continuation: skipped, claim_id: fixture.identity.claimId, claimed: true, due: true },
        ])
      } finally {
        await dropReferences()
      }
    })

    test('does not skip a vetoed page once the claim has expired', async () => {
      const fixture = await armBatch(['arm/vetoed-guard'])
      await expireClaim()
      try {
        await reference(fixture.rows[0].id)

        await expect(fixture.arm()).resolves.toBeUndefined()
        await expect(fixtures.readState(bucketId, 'continuation')).resolves.toEqual([
          { continuation: null },
        ])
      } finally {
        await dropReferences()
      }
    })

    test('persists the committed results without artifact keys', async () => {
      const fixture = await armBatch(['commit/no-keys'])
      await expect(fixture.arm()).resolves.toBeDefined()
      await expect(fixture.commit(fixture.armed)).resolves.toBeDefined()

      await expect(
        fixtures.readState(
          bucketId,
          "jsonb_path_exists(continuation, '$.batch.versions[*].artifacts[*].key') AS has_key, jsonb_array_length(continuation #> '{batch,versions}') AS versions"
        )
      ).resolves.toEqual([{ has_key: false, versions: 1 }])
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
    ])(
      'applies newerNoncurrentVersions at arm time with $label',
      async ({ newer, siblings, arms }) => {
        const fixture = await armBatch(['arm/newer'], {
          newerNoncurrentVersions: newer,
          snapshotAt: SNAPSHOT_AT,
        })
        await insertArchivedObjects(
          siblings.map((archivedAt) => ({ name: 'arm/newer', archivedAt }))
        )

        expect((await fixture.arm()) !== undefined).toBe(arms)
      }
    )

    test('does not count a newer sibling in another bucket at arm time', async () => {
      const fixture = await armBatch(['arm/newer-scope'], {
        newerNoncurrentVersions: 1,
        snapshotAt: SNAPSHOT_AT,
      })
      await insertOtherBucketSibling('arm/newer-scope', '2026-07-01T00:00:00.000Z')

      await expect(fixture.arm()).resolves.toBeUndefined()
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

    test('preserves an armed journal until its backend artifacts are resolved', async () => {
      const fixture = await armBatch(['completion/pending'])
      const row = fixture.rows[0]
      const { adapter, storage } = tHelper
      const root = storage.location.getRootLocation()
      const key = storage.location.getKeyLocation({
        tenantId: getConfig().tenantId,
        bucketId,
        objectName: row.name,
      })
      await adapter.uploadObject(
        root,
        key,
        row.version,
        Readable.from(['pending bytes']),
        'text/plain',
        'no-cache'
      )

      const armed = await fixture.arm()
      expect(armed?.continuation?.batch?.inFlight).toBeDefined()
      expect(await rowExists(row)).toBe(false)
      const before = await fixtures.readState(bucketId, 'continuation, claim_id, next_run_at')
      const nextRunAt = '2099-01-01T00:00:00.000Z'
      await expect(
        tHelper.database.completeLifecycleRun(
          { ...fixture.identity, generation: fixture.generation },
          {},
          nextRunAt
        )
      ).resolves.toBe(false)
      const changed = recordLifecycleArtifactOutcomes(
        armed!.continuation!,
        new Map([
          [armed!.continuation!.batch!.versions[0].artifacts[0].key, { outcome: 'DELETED' }],
        ])
      )
      for (const continuation of [null, fixture.staged, changed, armed!.continuation!]) {
        await expect(
          tHelper.database.releaseLifecycleClaim({ ...fixture.identity, continuation, nextRunAt })
        ).resolves.toBe(false)
      }
      await expect(
        fixtures.readState(bucketId, 'continuation, claim_id, next_run_at')
      ).resolves.toEqual(before)
      await expect(adapter.headObject(root, key, row.version)).resolves.toMatchObject({ size: 13 })

      await adapter.deleteObject(root, key, row.version)
      const [data, info] = armed!.continuation!.batch!.versions[0].artifacts
      const recorded = recordLifecycleArtifactOutcomes(
        armed!.continuation!,
        new Map([
          [data.key, { outcome: 'DELETED' as const }],
          [info.key, { outcome: 'ABSENT' as const }],
        ])
      )
      await expect(fixture.commit(recorded)).resolves.toBeDefined()
      await expect(
        tHelper.database.completeLifecycleRun(
          { ...fixture.identity, generation: fixture.generation },
          {},
          nextRunAt
        )
      ).resolves.toBe(true)
      await expect(adapter.headObject(root, key, row.version)).rejects.toThrow()
    })

    test('preserves an unknown journal on completion or explicit release', async () => {
      const fixture = await armBatch(['completion/unknown'])
      const continuation = { continuationVersion: 999 }
      await tHelper.database.connection.query(
        'UPDATE storage.bucket_lifecycle_states SET continuation = $2::jsonb WHERE bucket_id = $1',
        [bucketId, JSON.stringify(continuation)]
      )
      const nextRunAt = '2099-01-01T00:00:00.000Z'
      await expect(
        tHelper.database.completeLifecycleRun(
          { ...fixture.identity, generation: fixture.generation },
          {},
          nextRunAt
        )
      ).resolves.toBe(false)
      await expect(
        tHelper.database.releaseLifecycleClaim({
          ...fixture.identity,
          continuation: null,
          nextRunAt,
        })
      ).resolves.toBe(false)
      await expect(
        tHelper.database.releaseLifecycleClaim({ ...fixture.identity, nextRunAt })
      ).resolves.toBe(true)
      await expect(fixtures.readState(bucketId, 'continuation, claim_id')).resolves.toEqual([
        { continuation, claim_id: null },
      ])
    })

    test('preserves an in-flight journal with newer fields when releasing the claim', async () => {
      const fixture = await armBatch(['release/future-fields'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const nextRunAt = '2099-01-01T01:00:00.000Z'
      await tHelper.database.connection.query(
        `UPDATE storage.bucket_lifecycle_states
         SET continuation = jsonb_set(continuation, '{futureField}', '{"enabled":true}')
         WHERE bucket_id = $1`,
        [bucketId]
      )
      const stored = (await fixtures.readState(bucketId, 'continuation'))[0].continuation
      expect(stored.futureField).toEqual({ enabled: true })
      const decoded = await tHelper.database.revalidateLifecycleClaim(fixture.identity)
      expect(decoded?.continuation).not.toHaveProperty('futureField')

      await expect(
        tHelper.database.releaseLifecycleClaim({ ...fixture.identity, nextRunAt })
      ).resolves.toBe(true)
      await expect(
        fixtures.readState(bucketId, 'continuation, claim_id, next_run_at')
      ).resolves.toEqual([
        {
          continuation: stored,
          claim_id: null,
          next_run_at: new Date(nextRunAt),
        },
      ])
    })

    test('keeps a policy change made during an armed batch due after the stale run completes', async () => {
      const fixture = await armBatch(['policy/armed'])
      await expect(fixture.arm()).resolves.toBeDefined()
      await tHelper.database.putLifecycleConfiguration(bucketId, configuration(2))
      await expect(fixture.commit()).resolves.toBeDefined()

      await expect(
        tHelper.database.completeLifecycleRun(
          { ...fixture.identity, generation: fixture.generation },
          {},
          new Date(Date.now() + 86_400_000).toISOString()
        )
      ).resolves.toBe(true)
      await expect(dueNow()).resolves.toBe(true)
    })

    test('rejects a commit for a stale attempt', async () => {
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
      await expect(
        fixtures.readState(bucketId, "continuation #>> '{batch,inFlight,attemptId}' AS attempt")
      ).resolves.toEqual([{ attempt: fixture.armed.batch!.inFlight!.attemptId }])
    })

    test('abandons pending artifacts only for the claim holder and advances the cursor', async () => {
      const fixture = await armBatch(['abandon/persist'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const keys = fixture.armed.batch!.versions[0].artifacts.map((artifact) => artifact.key)
      const failed = recordLifecycleArtifactOutcomes(
        fixture.armed,
        new Map([
          [keys[0], { outcome: 'FAILED' as const, error: 'denied' }],
          [keys[1], { outcome: 'DELETED' as const }],
        ])
      )
      const commit = (claimId: string, abandonPending = false) =>
        tHelper.database.commitLifecycleAttempt({
          ...fixture.identity,
          claimId,
          attemptId: fixture.armed.batch!.inFlight!.attemptId,
          continuation: failed,
          abandonPending,
        })
      const abandoned = async () =>
        (
          await tHelper.database.connection.query(
            'SELECT key, name, version FROM storage.lifecycle_abandoned_artifacts WHERE bucket_id = $1',
            [bucketId]
          )
        ).rows

      const retained = await commit(fixture.identity.claimId)
      expect(retained?.continuation?.batch?.versions).toHaveLength(1)
      await expect(abandoned()).resolves.toEqual([])

      await expect(commit(randomUUID(), true)).resolves.toBeUndefined()
      expect(
        (await fixtures.readState(bucketId, 'continuation'))[0].continuation.batch
      ).toBeDefined()
      await expect(abandoned()).resolves.toEqual([])

      const result = await commit(fixture.identity.claimId, true)
      expect(result?.continuation?.batch).toBeUndefined()
      expect(result?.continuation).toMatchObject({
        cursor: fixture.staged.pageEnd,
        counters: { batchesCompleted: 1 },
      })
      await expect(abandoned()).resolves.toEqual([
        { key: keys[0], name: 'abandon/persist', version: fixture.rows[0].version },
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

    test.each(Object.keys(mutations) as Array<keyof typeof mutations>)(
      'does not arm a row that was %s after staging',
      async (kind) => {
        const fixture = await armBatch(['identity/pre-arm'])
        await mutateRow(kind, fixture.rows[0].id)

        await expect(fixture.arm()).resolves.toBeUndefined()
      }
    )

    test('rejects a commit whose version or artifact set differs from the armed batch', async () => {
      const fixture = await armBatch(['commit/a', 'commit/b'])
      await expect(fixture.arm()).resolves.toBeDefined()
      const recorded = fixture.recorded()

      const dropped = {
        ...recorded,
        batch: { ...recorded.batch!, versions: recorded.batch!.versions.slice(0, 1) },
      }
      const withoutInfo = {
        ...recorded,
        batch: {
          ...recorded.batch!,
          versions: recorded.batch!.versions.map((version, index) =>
            index === 0 ? { ...version, artifacts: version.artifacts.slice(0, 1) } : version
          ),
        },
      }
      const retargeted = (changes: Record<string, string>) => ({
        ...recorded,
        batch: {
          ...recorded.batch!,
          versions: recorded.batch!.versions.map((version, index) =>
            index === 0 ? { ...version, ...changes } : version
          ),
        },
      })
      for (const proposed of [
        dropped,
        withoutInfo,
        { ...recorded, batch: { ...recorded.batch!, keySeparator: '-$v-' as const } },
        retargeted({ id: randomUUID() }),
        retargeted({ name: 'commit/other' }),
        retargeted({ version: randomUUID() }),
      ]) {
        await expect(fixture.commit(proposed)).rejects.toMatchObject({
          code: 'InvalidRequest',
          message: expect.stringContaining('artifact set changed'),
        })
      }
      await expect(fixture.commit()).resolves.toBeDefined()
    })

    test.each(['name', 'version', 'bucket_id'] as const)(
      'does not arm a row whose %s changes while arm waits for its row lock',
      async (column) => {
        const fixture = await armBatch(['lock/changed'])
        const replacement = column === 'bucket_id' ? await fixtures.createBucket() : randomUUID()
        const held = await holdWriter(
          `UPDATE storage.objects SET ${column} = $4 WHERE bucket_id = $1 AND name = $2 AND version = $3`,
          [bucketId, fixture.rows[0].name, fixture.rows[0].version, replacement]
        )
        try {
          const arming = fixture.arm()
          await held.blocked()
          await held.finish('COMMIT')
          await expect(arming).resolves.toBeUndefined()
          const rows = await tHelper.database.connection.query(
            `SELECT 1 FROM storage.objects WHERE ${column} = $1`,
            [replacement]
          )
          expect(rows.rowCount).toBe(1)
        } finally {
          await held.writer.end().catch(() => undefined)
        }
      }
    )

    test.each([
      { isolation: 'READ COMMITTED', code: undefined },
      { isolation: 'REPEATABLE READ', code: '40001' },
      { isolation: 'SERIALIZABLE', code: '40001' },
    ])('a $isolation promoter racing arm finds the row deleted', async ({ isolation, code }) => {
      const fixture = await armBatch(['arm/promoter'])
      const promoter = new Client({ connectionString: getConfig().databaseURL })
      await promoter.connect()
      try {
        await promoter.query(`BEGIN ISOLATION LEVEL ${isolation}`)
        await promoter.query('SELECT 1')
        await expect(fixture.arm()).resolves.toBeDefined()
        const promotion = promoter.query(
          'UPDATE storage.objects SET archived_at = NULL WHERE id = $1',
          [fixture.rows[0].id]
        )
        if (code) await expect(promotion).rejects.toMatchObject({ code })
        else await expect(promotion).resolves.toMatchObject({ rowCount: 0 })
      } finally {
        await promoter.query('ROLLBACK').catch(() => undefined)
        await promoter.end()
      }
      expect(await rowExists(fixture.rows[0])).toBe(false)
    })

    test.each([
      { holder: 'object advisory lock' },
      { holder: 'bucket row lock' },
      { holder: 'object row lock' },
    ])(
      'fails arm with LockTimeout after about 5s behind a $holder',
      async ({ holder }) => {
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
        } finally {
          release.resolve()
          await holding
          await writer?.finish('ROLLBACK')
        }
      },
      30_000
    )

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

    test.each([
      {
        by: 'name',
        names: ['lock/order/c', 'lock/order/a', 'lock/order/B'],
        versions: undefined,
      },
      {
        by: 'version',
        names: ['lock/order/x', 'lock/order/x', 'lock/order/x'],
        versions: ['c', 'a', 'B'],
      },
    ])('locks staged rows in $by order regardless of batch order', async ({ names, versions }) => {
      const fixture = await armBatch(names, { versions })
      await tHelper.database.connection.query(
        `INSERT INTO storage.objects (bucket_id, name, version, is_versioned, metadata)
         SELECT $1, 'filler/' || g, gen_random_uuid()::text, true, '{"size":1}'
         FROM generate_series(1, 1000) AS g`,
        [bucketId]
      )
      await tHelper.database.connection.query('ANALYZE storage.objects')
      const held = await holdWriter('SELECT 1 FROM storage.objects WHERE id = $1 FOR UPDATE', [
        fixture.rows[1].id,
      ])
      try {
        const arming = fixture.arm()
        await held.blocked()
        const free = await tHelper.database.connection.query(
          `SELECT name, version FROM storage.objects
           WHERE bucket_id = $1 AND archived_at IS NOT NULL
           FOR UPDATE SKIP LOCKED`,
          [bucketId]
        )
        expect(free.rows).toEqual([{ name: names[0], version: fixture.rows[0].version }])
        await held.finish('ROLLBACK')
        await expect(arming).resolves.toBeDefined()
      } finally {
        await held.writer.end().catch(() => undefined)
      }
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

  test.each([
    {
      by: 'name',
      names: ['order/a', 'order/B', 'order/c'],
      versions: [randomUUID(), randomUUID(), randomUUID()],
    },
    { by: 'version', names: ['order/x', 'order/x', 'order/x'], versions: ['a', 'B', 'c'] },
  ])('locks id-batch deletions in the same $by order as arming', async ({ names, versions }) => {
    const ids = [
      'ffffffff-0000-4000-8000-000000000000',
      '88888888-0000-4000-8000-000000000000',
      '00000000-0000-4000-8000-000000000000',
    ]
    await tHelper.database.connection.query(
      `INSERT INTO storage.objects (id, bucket_id, name, version, is_versioned, metadata, archived_at)
       SELECT id::uuid, $1, name, v, true, '{"size":1}', now() - i * interval '1 second'
       FROM unnest($2::text[], $3::text[], $4::text[]) WITH ORDINALITY AS t(name, v, id, i)
       ORDER BY i DESC`,
      [bucketId, names, versions, ids]
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
          'SELECT id FROM storage.objects WHERE bucket_id = $1 FOR UPDATE SKIP LOCKED',
          [bucketId]
        )
      ).rows.map((row) => row.id)
      expect(free).toEqual(expect.arrayContaining([ids[0], ids[2]]))
    } finally {
      await holder.query('ROLLBACK')
      await deleting
      await holder.end()
      await observer.end()
    }
  })

  describe('abandoned artifacts', () => {
    const seed = (bucket: string, rows: Array<[string, string]>) =>
      tHelper.database.connection.query(
        `INSERT INTO storage.lifecycle_abandoned_artifacts (bucket_id, key, name, attempted_at)
         SELECT $1, r.key, r.key, r.attempted_at::timestamptz
         FROM unnest($2::text[], $3::text[]) AS r(key, attempted_at)`,
        [bucket, rows.map(([key]) => key), rows.map(([, attemptedAt]) => attemptedAt)]
      )

    test('lists tenant-wide by attempt age and resolves bucket and key pairs', async () => {
      await seed(bucketId, [
        ['a', '2026-01-01T00:00:01Z'],
        ['b', '2026-01-01T00:00:03Z'],
        ['c', '2026-01-01T00:00:05Z'],
      ])
      const other = await fixtures.createBucket()
      await seed(other, [
        ['a', '2026-01-01T00:00:02Z'],
        ['z', '2026-01-01T00:00:04Z'],
      ])
      const list = (attemptedBefore: string, limit = 10) =>
        tHelper.database.listLifecycleAbandonedArtifacts(attemptedBefore, limit)

      await expect(list('2026-01-01T00:00:04Z')).resolves.toEqual([
        { bucketId, key: 'a', live: false },
        { bucketId: other, key: 'a', live: false },
        { bucketId, key: 'b', live: false },
      ])
      await expect(list('2026-01-02T00:00:00Z', 2)).resolves.toEqual([
        { bucketId, key: 'a', live: false },
        { bucketId: other, key: 'a', live: false },
      ])

      await tHelper.database.resolveLifecycleAbandonedArtifacts(
        [{ bucketId: other, key: 'a' }],
        [{ bucketId, key: 'a' }]
      )

      await expect(list('2026-01-02T00:00:00Z')).resolves.toEqual([
        { bucketId, key: 'b', live: false },
        { bucketId: other, key: 'z', live: false },
        { bucketId, key: 'c', live: false },
      ])
      await expect(list(new Date(Date.now() + 60_000).toISOString())).resolves.toEqual([
        { bucketId, key: 'b', live: false },
        { bucketId: other, key: 'z', live: false },
        { bucketId, key: 'c', live: false },
        { bucketId, key: 'a', live: false },
      ])
    })

    describe('live object backstop', () => {
      const future = () => new Date(Date.now() + 60_000).toISOString()
      const remaining = async () =>
        (
          await tHelper.database.connection.query(
            'SELECT key FROM storage.lifecycle_abandoned_artifacts WHERE bucket_id = $1 ORDER BY key',
            [bucketId]
          )
        ).rows.map((row) => row.key)
      const record = (key: string, name: string, version: string | null) =>
        tHelper.database.connection.query(
          `INSERT INTO storage.lifecycle_abandoned_artifacts (bucket_id, key, name, version)
           VALUES ($1, $2, $3, $4)`,
          [bucketId, key, name, version]
        )
      const liveObject = (name: string, version: string | null) =>
        tHelper.database.connection.query(
          `INSERT INTO storage.objects (bucket_id, name, version, is_versioned, archived_at, metadata)
           VALUES ($1, $2, $3::text, $3::text IS NOT NULL, CASE WHEN $3::text IS NULL THEN NULL ELSE now() END, '{"size":1}')`,
          [bucketId, name, version]
        )

      test('drops records owned by a live object with the same name and version', async () => {
        const owned = randomUUID()
        const orphan = randomUUID()
        const reversioned = randomUUID()
        await liveObject('guard/a', owned)
        await liveObject('guard/c', randomUUID())
        await record(`guard/a/${owned}`, 'guard/a', owned)
        await record(`guard/a/${owned}.info`, 'guard/a', owned)
        await record(`guard/b/${orphan}`, 'guard/b', orphan)
        await record(`guard/c/${reversioned}`, 'guard/c', reversioned)

        await expect(
          tHelper.database.listLifecycleAbandonedArtifacts(future(), 10)
        ).resolves.toEqual([
          { bucketId, key: `guard/a/${owned}`, live: true },
          { bucketId, key: `guard/a/${owned}.info`, live: true },
          { bucketId, key: `guard/b/${orphan}`, live: false },
          { bucketId, key: `guard/c/${reversioned}`, live: false },
        ])
        await expect(remaining()).resolves.toEqual([`guard/b/${orphan}`, `guard/c/${reversioned}`])
      })

      test('matches a null-version record only to a live null-version row', async () => {
        const other = randomUUID()
        await liveObject('guard/legacy', null)
        await liveObject('guard/other', other)
        await record('guard/legacy', 'guard/legacy', null)
        await record('guard/other', 'guard/other', null)

        await expect(
          tHelper.database.listLifecycleAbandonedArtifacts(future(), 10)
        ).resolves.toEqual([
          { bucketId, key: 'guard/legacy', live: true },
          { bucketId, key: 'guard/other', live: false },
        ])
        await expect(remaining()).resolves.toEqual(['guard/other'])
      })
    })

    test('survives bucket deletion', async () => {
      const other = await fixtures.createBucket()
      await seed(other, [['a', '2026-01-01T00:00:00Z']])

      await withDeleteEnabled(tHelper.database.connection, (transaction) =>
        transaction.query('DELETE FROM storage.buckets WHERE id = $1', [other])
      )

      await expect(
        tHelper.database.listLifecycleAbandonedArtifacts('2026-01-02T00:00:00Z', 10)
      ).resolves.toEqual([{ bucketId: other, key: 'a', live: false }])
    })

    test('reports whether any abandoned artifact exists', async () => {
      const hasAbandoned = async () =>
        (await tHelper.database.findLifecycleDispatchState({ runBudgetMs: 20_000 }))
          .hasAbandonedArtifacts

      await expect(hasAbandoned()).resolves.toBe(false)
      await seed(bucketId, [['a', '2026-01-01T00:00:00Z']])
      await expect(hasAbandoned()).resolves.toBe(true)
      await tHelper.database.resolveLifecycleAbandonedArtifacts([{ bucketId, key: 'a' }], [])
      await expect(hasAbandoned()).resolves.toBe(false)
    })
  })
})
