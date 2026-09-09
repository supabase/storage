import { randomUUID } from 'node:crypto'
import {
  armLifecycleAttempt,
  createLifecycleContinuation,
  freezeLifecycleBatchVersion,
  normalizeLifecycleConfiguration,
  recordLifecycleArtifactOutcomes,
  stageLifecycleBatch,
} from '@storage/lifecycle'
import { useLifecycleVersioningFixtures } from './utils/lifecycle-versioning'
import { useStorage, withDeleteEnabled } from './utils/storage'

describe('bucket lifecycle controls', () => {
  const helper = useStorage()
  const fixtures = useLifecycleVersioningFixtures()
  let bucketId: string

  beforeEach(async () => {
    bucketId = fixtures.trackBucket(`bucket-lifecycle-${randomUUID()}`)
    await helper.database.createBucket({ id: bucketId, name: bucketId })
  })

  afterEach(async () => {
    await withDeleteEnabled(helper.database.connection, async (transaction) => {
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.bucket_lifecycle_states WHERE bucket_id = $1', [
        bucketId,
      ])
      await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
    })
  })

  const configuration = {
    rules: [
      {
        id: 'expire-history',
        status: 'Enabled' as const,
        filter: {},
        noncurrentVersionExpiration: {
          noncurrentDays: 30,
          newerNoncurrentVersions: 2,
        },
      },
    ],
  }

  it('preserves generation and continuation for a canonically unchanged PUT', async () => {
    const first = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    expect(first.lifecycle_configuration_generation).toMatch(/^[0-9a-f]{8}-[0-9a-f-]{27}$/i)

    const marker = { preserved: true }
    await helper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states
       SET continuation = $2::jsonb,
           next_run_at = '2030-01-01T00:00:00Z',
           failure_count = 3
       WHERE bucket_id = $1`,
      [bucketId, JSON.stringify(marker)]
    )

    const retry = await helper.database.putLifecycleConfiguration(bucketId, {
      rules: configuration.rules.map((rule) => ({ ...rule, filter: {} })),
    })
    expect(retry.lifecycle_configuration_generation).toBe(first.lifecycle_configuration_generation)

    const state = await helper.database.connection.query<{
      continuation: unknown
      next_run_at: Date
      failure_count: number
    }>(
      `SELECT continuation, next_run_at, failure_count
       FROM storage.bucket_lifecycle_states WHERE bucket_id = $1`,
      [bucketId]
    )
    expect(state.rows[0]).toMatchObject({ continuation: marker, failure_count: 3 })
    expect(state.rows[0].next_run_at.toISOString()).toBe('2030-01-01T00:00:00.000Z')
  })

  it('preserves generation when an ID-less configuration is retried', async () => {
    const idlessConfiguration = normalizeLifecycleConfiguration({
      rules: [
        {
          status: 'Enabled' as const,
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
      ],
    })

    const first = await helper.database.putLifecycleConfiguration(bucketId, idlessConfiguration)
    const retry = await helper.database.putLifecycleConfiguration(bucketId, {
      rules: idlessConfiguration.rules.map((rule) => ({ ...rule, filter: {} })),
    })

    expect(first.lifecycle_configuration?.rules[0].id).toMatch(/^rule-[0-9a-f]{64}$/)
    expect(retry.lifecycle_configuration_generation).toBe(first.lifecycle_configuration_generation)
    expect(retry.lifecycle_configuration?.rules[0].id).toBe(
      first.lifecycle_configuration?.rules[0].id
    )
  })

  it('treats rule order as irrelevant to configuration generation', async () => {
    const rules = [
      configuration.rules[0],
      {
        id: 'expire-sooner',
        status: 'Enabled' as const,
        filter: {},
        noncurrentVersionExpiration: { noncurrentDays: 7 },
      },
    ]
    const first = await helper.database.putLifecycleConfiguration(bucketId, { rules })
    const marker = { preservedAcrossReorder: true }
    await helper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states
       SET continuation = $2::jsonb
       WHERE bucket_id = $1`,
      [bucketId, JSON.stringify(marker)]
    )

    const reordered = await helper.database.putLifecycleConfiguration(bucketId, {
      rules: [...rules].reverse(),
    })

    expect(reordered.lifecycle_configuration_generation).toBe(
      first.lifecycle_configuration_generation
    )
    expect(reordered.lifecycle_configuration?.rules.map((rule) => rule.id)).toEqual([
      'expire-history',
      'expire-sooner',
    ])
    const state = await helper.database.connection.query<{ continuation: unknown }>(
      `SELECT continuation
       FROM storage.bucket_lifecycle_states
       WHERE bucket_id = $1`,
      [bucketId]
    )
    expect(state.rows[0].continuation).toEqual(marker)
  })

  it('keeps disabled state dormant until explicitly woken after fixture activation', async () => {
    const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    const dormant = await helper.database.connection.query<{ next_run_at: Date | null }>(
      'SELECT next_run_at FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
      [bucketId]
    )
    expect(dormant.rows[0].next_run_at).toBeNull()
    await expect(helper.database.findNextLifecycleDispatchAt()).resolves.toBeNull()

    await fixtures.setStatus(bucketId, 'ENABLED')
    await expect(helper.database.wakeLifecycleShards(bucketId)).resolves.toEqual([
      { bucketId, scanKind: 'NONCURRENT', shardEpoch: '1', shardId: 0 },
    ])
    const enabled = await helper.database.findLifecycleBucket(bucketId)
    expect(enabled).toMatchObject({ versioning_status: 'ENABLED' })
    expect(enabled.lifecycle_configuration_generation).toBe(
      configured.lifecycle_configuration_generation
    )

    const due = await helper.database.connection.query<{ next_run_at: Date | null }>(
      'SELECT next_run_at FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
      [bucketId]
    )
    expect(due.rows[0].next_run_at).toBeInstanceOf(Date)
    await expect(helper.database.findNextLifecycleDispatchAt()).resolves.toEqual(expect.any(String))

    await helper.database.wakeLifecycleShards(bucketId)
    const retry = await helper.database.findLifecycleBucket(bucketId)
    expect(retry).toMatchObject({
      versioning_status: 'ENABLED',
      lifecycle_configuration_generation: enabled.lifecycle_configuration_generation,
    })
  })

  it.each([
    'DISABLED',
    'ENABLED',
    'SUSPENDED',
  ] as const)('deletes lifecycle state with an empty %s bucket', async (status) => {
    await fixtures.setStatus(bucketId, status)
    await helper.database.putLifecycleConfiguration(bucketId, configuration)

    await expect(helper.storage.deleteBucket(bucketId)).resolves.toBe(1)
    await expect(
      helper.database.connection.query(
        'SELECT 1 FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
        [bucketId]
      )
    ).resolves.toMatchObject({ rowCount: 0 })
    await expect(
      helper.database.connection.query('SELECT 1 FROM storage.buckets WHERE id = $1', [bucketId])
    ).resolves.toMatchObject({ rowCount: 0 })
  })

  it('requires lifecycle state cleanup to stay inside the bucket-delete transaction', async () => {
    await expect(
      helper.database.asSuperUser().prepareLifecycleStateForBucketDelete(bucketId)
    ).rejects.toMatchObject({ code: 'InternalError' })
  })

  it('preserves unknown lifecycle state when deleting an unversioned bucket', async () => {
    await helper.database.putLifecycleConfiguration(bucketId, configuration)
    await helper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states
       SET continuation = '{"continuationVersion":999}'::jsonb
       WHERE bucket_id = $1`,
      [bucketId]
    )

    await expect(helper.storage.deleteBucket(bucketId)).rejects.toMatchObject({
      code: 'ResourceReferenced',
    })
    await expect(
      helper.database.connection.query(
        'SELECT 1 FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
        [bucketId]
      )
    ).resolves.toMatchObject({ rowCount: 1 })
    await expect(
      helper.database.connection.query('SELECT 1 FROM storage.buckets WHERE id = $1', [bucketId])
    ).resolves.toMatchObject({ rowCount: 1 })
  })

  it('retains an armed attempt when the lifecycle configuration is deleted', async () => {
    const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    const generation = configured.lifecycle_configuration_generation!
    const now = new Date().toISOString()
    const continuation = armLifecycleAttempt(
      stageLifecycleBatch(
        createLifecycleContinuation({
          runId: randomUUID(),
          trigger: 'configuration_change',
          generation,
          snapshotAt: now,
          topology: {
            scanKind: 'NONCURRENT',
            epoch: '1',
            shardId: 0,
            shardCount: 1,
          },
        }),
        {
          pageEnd: { name: 'object', archivedAt: now },
          rawRowsExamined: 1,
          versions: [
            {
              name: 'object',
              version: randomUUID(),
              artifacts: [
                { key: 'object/version', outcome: 'UNRESOLVED' },
                { key: 'object/version.info', outcome: 'UNRESOLVED' },
              ],
            },
          ],
        }
      ),
      { attemptId: randomUUID(), authorizedGeneration: generation, startedAt: now }
    )
    await helper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states
       SET continuation = $2::jsonb, next_run_at = clock_timestamp()
       WHERE bucket_id = $1`,
      [bucketId, JSON.stringify(continuation)]
    )

    const deleted = await helper.database.deleteLifecycleConfiguration(bucketId)
    expect(deleted).toMatchObject({ lifecycle_configuration: null })

    const state = await helper.database.connection.query<{
      continuation: unknown
      next_run_at: Date | null
    }>(
      'SELECT continuation, next_run_at FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
      [bucketId]
    )
    expect(state.rows[0].continuation).toEqual(continuation)
    expect(state.rows[0].next_run_at).toBeInstanceOf(Date)

    const retry = await helper.database.deleteLifecycleConfiguration(bucketId)
    expect(retry.lifecycle_configuration).toBeNull()
    expect(
      await helper.database.connection.query(
        'SELECT 1 FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
        [bucketId]
      )
    ).toMatchObject({ rowCount: 1 })
  })

  it('rejects lifecycle controls for a resolved analytics bucket', async () => {
    await helper.database.connection.query(
      `UPDATE storage.buckets SET type = 'ANALYTICS' WHERE id = $1`,
      [bucketId]
    )

    await expect(
      helper.database.putLifecycleConfiguration(bucketId, configuration)
    ).rejects.toMatchObject({
      code: 'InvalidRequest',
      httpStatusCode: 400,
    })
  })

  it('commits partial artifact outcomes before advancing archived metadata', async () => {
    const configured = await helper.database.putLifecycleConfiguration(bucketId, {
      rules: [{ ...configuration.rules[0], noncurrentVersionExpiration: { noncurrentDays: 1 } }],
    })
    await fixtures.setStatus(bucketId, 'ENABLED')
    await helper.database.wakeLifecycleShards(bucketId)
    const generation = configured.lifecycle_configuration_generation!
    const name = 'history/expired.txt'
    const version = randomUUID()
    const completedName = 'history/completed.txt'
    const completedVersion = randomUUID()
    const transaction = await helper.database.connection.transaction()
    try {
      await helper.database.connection.setScope(transaction)

      await transaction.query(
        `INSERT INTO storage.objects (
           id, bucket_id, name, version, metadata, created_at, updated_at,
           archived_at, is_versioned, is_delete_marker
         ) VALUES (
           $1, $2, $3, $4, $5::jsonb, $6, $6, $7, true, false
         )`,
        [
          randomUUID(),
          bucketId,
          name,
          version,
          JSON.stringify({ size: 42 }),
          '2026-07-01T00:00:01.000Z',
          '2026-07-02T00:00:00.000Z',
        ]
      )
      await transaction.query(
        `INSERT INTO storage.objects (
           id, bucket_id, name, version, metadata, created_at, updated_at,
           archived_at, is_versioned, is_delete_marker
         ) VALUES (
           $1, $2, $3, $4, $5::jsonb, $6, $6, $7, true, false
         )`,
        [
          randomUUID(),
          bucketId,
          completedName,
          completedVersion,
          JSON.stringify({ size: 8 }),
          '2026-07-01T00:00:01.000Z',
          '2026-07-02T00:00:00.000Z',
        ]
      )
      await transaction.commit()
    } catch (error) {
      await transaction.rollback()
      throw error
    }

    const claimId = randomUUID()
    const claimed = await helper.database.claimLifecycleShard({
      bucketId,
      scanKind: 'NONCURRENT',
      shardId: 0,
      shardEpoch: '1',
      claimId,
      leaseMs: 120_000,
    })
    expect(claimed).toMatchObject({ claimId })
    const candidate = {
      id: randomUUID(),
      bucketId,
      name,
      version,
      isVersioned: true,
      isDeleteMarker: false,
      metadata: { size: 42 },
      createdAt: '2026-07-01T00:00:01.000Z',
      archivedAt: '2026-07-02T00:00:00.000Z',
    }
    const completedCandidate = {
      ...candidate,
      id: randomUUID(),
      name: completedName,
      version: completedVersion,
      metadata: { size: 8 },
    }
    const staged = stageLifecycleBatch(
      createLifecycleContinuation({
        runId: randomUUID(),
        trigger: 'manual',
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
        pageEnd: {
          name: completedName,
          archivedAt: completedCandidate.archivedAt,
        },
        rawRowsExamined: 2,
        versions: [
          freezeLifecycleBatchVersion(candidate),
          freezeLifecycleBatchVersion(completedCandidate),
        ],
      }
    )
    expect(
      await helper.database.saveLifecycleContinuation(
        { bucketId, scanKind: 'NONCURRENT', shardId: 0, shardEpoch: '1', claimId },
        staged
      )
    ).toBe(true)
    const armed = armLifecycleAttempt(staged, {
      attemptId: randomUUID(),
      authorizedGeneration: generation,
      startedAt: new Date().toISOString(),
    })
    const armedState = await helper.database.armLifecycleAttempt({
      bucketId,
      scanKind: 'NONCURRENT',
      shardId: 0,
      shardEpoch: '1',
      claimId,
      configurationGeneration: generation,
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
    const partial = await helper.database.commitLifecycleAttempt({
      bucketId,
      scanKind: 'NONCURRENT',
      shardId: 0,
      shardEpoch: '1',
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
      await helper.database.connection.query(
        'SELECT 1 FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
        [bucketId, name, version]
      )
    ).toMatchObject({ rowCount: 1 })
    expect(
      await helper.database.connection.query(
        'SELECT 1 FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
        [bucketId, completedName, completedVersion]
      )
    ).toMatchObject({ rowCount: 0 })

    const recorded = recordLifecycleArtifactOutcomes(
      partial!.continuation!,
      new Map([[armed.batch!.versions[0].artifacts[1].key, { outcome: 'DELETED' as const }]])
    )
    const committed = await helper.database.commitLifecycleAttempt({
      bucketId,
      scanKind: 'NONCURRENT',
      shardId: 0,
      shardEpoch: '1',
      claimId,
      attemptId: armed.batch!.inFlight!.attemptId,
      continuation: recorded,
    })
    expect(committed?.continuation).toMatchObject({
      cursor: { name: completedName, archivedAt: candidate.archivedAt },
      counters: { objectVersionsDeleted: 2, bytesDeleted: 50, batchesCompleted: 1 },
    })
    expect(
      await helper.database.connection.query(
        'SELECT 1 FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
        [bucketId, name, version]
      )
    ).toMatchObject({ rowCount: 0 })
  })
})
