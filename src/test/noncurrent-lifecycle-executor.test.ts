import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import { text } from 'node:stream/consumers'
import { logSchema } from '@internal/monitoring'
import { isMissingBackendObject, withOptionalVersion } from '@storage/backend'
import { StoragePgDB } from '@storage/database'
import type { LifecycleBucketRunPayload } from '@storage/events/lifecycle-rules/bucket-run'
import { retryAbandonedLifecycleArtifacts } from '@storage/lifecycle/abandoned-artifacts'
import {
  type LifecycleExecutorOptions,
  NoncurrentLifecycleExecutor,
} from '@storage/lifecycle/executor'
import { Client } from 'pg'
import PgBoss from 'pg-boss'
import { getConfig } from '../config'
import { batchOf, newContinuation } from './utils/lifecycle-continuation'
import { useLifecycleStorage } from './utils/lifecycle-versioning'
import { withDeleteEnabled } from './utils/storage'

const { storageBackendType, storageS3Bucket, tenantId } = getConfig()
const HOUR_MS = 60 * 60 * 1000
const DAY_MS = 24 * HOUR_MS
const threeDaysAgo = () => new Date(Date.now() - 3 * DAY_MS)

describe('noncurrent lifecycle executor against version-aware writes', () => {
  const fixtures = useLifecycleStorage('lifecycle-executor')
  const helper = fixtures.helper
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

  beforeEach(() => {
    bucketId = fixtures.bucketId
  })

  afterEach(async () => {
    const cleanup = await Promise.allSettled(
      uploaded
        .splice(0)
        .map(({ name, version }) =>
          helper.adapter.deleteObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, version)
        )
    )
    expect(cleanup.filter((result) => result.status === 'rejected')).toEqual([])
  })

  async function uploadFixture(name: string, version: string | null) {
    uploaded.push({ name, version })
    return helper.adapter.uploadObject(
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
    const archivedMetadata = await uploadFixture(name, archivedVersion)
    if (archivedVersion === null) {
      // Legacy unsuffixed blobs predate the UUID writer.
      await helper.database.connection.query(
        `INSERT INTO storage.objects (bucket_id, name, version, is_versioned, metadata)
         VALUES ($1, $2, NULL, false, $3::jsonb)`,
        [bucketId, name, JSON.stringify(archivedMetadata)]
      )
    } else {
      if (options.isVersioned !== false) await fixtures.setStatus(bucketId, 'ENABLED')
      await helper.uploader.completeUpload({
        bucketId,
        objectName: name,
        version: archivedVersion,
        objectMetadata: archivedMetadata,
        uploadType: 'standard',
        isUpsert: true,
      })
    }
    await fixtures.setStatus(bucketId, 'ENABLED')
    // File storage cannot retain an unsuffixed blob beside a revision directory.
    let currentMetadata = archivedMetadata
    if (archivedVersion !== null || storageBackendType === 's3') {
      currentMetadata = await uploadFixture(name, currentVersion)
    }
    await helper.uploader.completeUpload({
      bucketId,
      objectName: name,
      version: currentVersion,
      objectMetadata: currentMetadata,
      uploadType: 'standard',
      isUpsert: true,
    })
    if (options.archivedAt) {
      await helper.database.connection.query(
        `UPDATE storage.objects SET archived_at = $4
         WHERE bucket_id = $1 AND name = $2 AND version IS NOT DISTINCT FROM $3`,
        [bucketId, name, archivedVersion, options.archivedAt]
      )
    }
  }

  const historyRows = (names: string[]) =>
    names.map((name) => ({ name, archived: randomUUID(), current: randomUUID() }))

  async function activate(status: 'ENABLED' | 'SUSPENDED' = 'ENABLED') {
    await fixtures.setStatus(bucketId, status)
    await helper.database.putLifecycleConfiguration(bucketId, configuration)
  }

  function runExecutor(
    options: Partial<LifecycleExecutorOptions> & {
      tenantLifecycleEnabled?: boolean
      signal?: AbortSignal
    } = {}
  ) {
    const { tenantLifecycleEnabled = true, signal, ...executorOptions } = options
    return new NoncurrentLifecycleExecutor(helper.storage, {
      now: () => new Date(Date.now() + 3 * DAY_MS),
      ...executorOptions,
    }).run(bucketId, { tenantLifecycleEnabled, signal })
  }

  const makeDue = () =>
    helper.database.connection.query(
      "UPDATE storage.bucket_lifecycle_states SET next_run_at = '2000-01-01' WHERE bucket_id = $1",
      [bucketId]
    )

  it('evaluates a retained-history page once while deleting one candidate per transaction', async () => {
    await activate()
    await helper.database.putLifecycleConfiguration(bucketId, {
      rules: [
        {
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 1, newerNoncurrentVersions: 1 },
        },
      ],
    })
    await helper.database.connection.query(
      `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned, is_delete_marker)
       SELECT $1, 'evaluated/' || lpad(n::text, 3, '0'), gen_random_uuid()::text, at, true, true
       FROM generate_series(1, 101) AS n,
         unnest(ARRAY['2000-01-01'::timestamptz, '2000-01-02'::timestamptz]) AS at`,
      [bucketId]
    )
    const evaluation = vi.spyOn(StoragePgDB.prototype, 'evaluateNoncurrentLifecyclePage')
    await expect(runExecutor({ pageSize: 480, deleteBatchSize: 1 })).resolves.toMatchObject({
      status: 'COMPLETED',
    })
    expect(evaluation).toHaveBeenCalledTimes(1)
    const [state] = await fixtures.readState(bucketId, 'last_result')
    expect(state.last_result.counters).toMatchObject({
      versionsExamined: 202,
      versionsEligible: 101,
      deleteMarkersDeleted: 101,
      batchesCompleted: 101,
    })
    const remaining = await helper.database.connection.query(
      `SELECT count(*)::integer AS count FROM storage.objects
       WHERE bucket_id = $1 AND archived_at = '2000-01-02'`,
      [bucketId]
    )
    expect(remaining.rows).toEqual([{ count: 101 }])
  }, 30_000)

  it('resumes within one object key without rounding a chunk cursor to milliseconds', async () => {
    await activate()
    const name = 'buffer/microseconds'
    const archived = [randomUUID(), randomUUID(), randomUUID()]
    for (const [index, version] of archived.entries()) {
      const metadata = await uploadFixture(name, version)
      await helper.database.connection.query(
        `INSERT INTO storage.objects(bucket_id, name, version, archived_at, is_versioned, metadata)
         VALUES ($1, $2, $3, $4, true, $5)`,
        [
          bucketId,
          name,
          version,
          `2000-01-01T00:00:00.00000${index + 1}Z`,
          JSON.stringify(metadata),
        ]
      )
    }
    const controller = new AbortController()
    const original = helper.adapter.deleteObjectsDetailed.bind(helper.adapter)
    vi.spyOn(helper.adapter, 'deleteObjectsDetailed').mockImplementationOnce(async (...args) => {
      const result = await original(...args)
      controller.abort()
      return result
    })
    await expect(
      runExecutor({ pageSize: 100, deleteBatchSize: 1, signal: controller.signal })
    ).resolves.toMatchObject({ status: 'PARTIAL' })
    const [state] = await fixtures.readState(bucketId, 'continuation')
    expect(state.continuation.cursor).toEqual({ name, archivedAt: '2000-01-01T00:00:00.000001Z' })
    await makeDue()
    await expect(runExecutor({ pageSize: 100, deleteBatchSize: 1 })).resolves.toMatchObject({
      status: 'COMPLETED',
    })
    expect(await versions(name)).toEqual([])
    const [completed] = await fixtures.readState(bucketId, 'last_result')
    expect(completed.last_result.counters).toMatchObject({
      versionsExamined: 3,
      objectVersionsDeleted: 3,
    })
    for (const version of archived) await expectBlobAbsent(name, version)
  })

  it.each(['budget', 'abort'] as const)(
    'resumes the buffered suffix after %s stops between chunks',
    async (stop) => {
      await activate()
      const rows = historyRows(['buffer/a', 'buffer/b', 'buffer/c'])
      for (const row of rows)
        await seedHistory(row.name, row.archived, row.current, { archivedAt: threeDaysAgo() })
      let now = Date.now() + 3 * DAY_MS
      const controller = new AbortController()
      const original = helper.adapter.deleteObjectsDetailed.bind(helper.adapter)
      vi.spyOn(helper.adapter, 'deleteObjectsDetailed').mockImplementationOnce(async (...args) => {
        const result = await original(...args)
        if (stop === 'abort') controller.abort()
        else now += 1000
        return result
      })
      const evaluation = vi.spyOn(StoragePgDB.prototype, 'evaluateNoncurrentLifecyclePage')
      await expect(
        runExecutor({
          pageSize: 100,
          deleteBatchSize: 1,
          jobBudgetMs: 1000,
          now: () => new Date(now),
          signal: controller.signal,
        })
      ).resolves.toMatchObject({ status: 'PARTIAL' })
      expect(evaluation).toHaveBeenCalledTimes(1)
      const [state] = await fixtures.readState(bucketId, 'continuation')
      expect(state.continuation).toMatchObject({
        cursor: { name: rows[0].name },
        counters: { versionsExamined: 1, versionsEligible: 1, objectVersionsDeleted: 1 },
      })
      expect(state.continuation.batch).toBeUndefined()
      await expectBlobAbsent(rows[0].name, rows[0].archived)
      for (const row of rows.slice(1)) expect(await versions(row.name)).toHaveLength(2)
      await makeDue()
      await expect(runExecutor({ pageSize: 480, deleteBatchSize: 2 })).resolves.toMatchObject({
        status: 'COMPLETED',
      })
      expect(evaluation).toHaveBeenCalledTimes(2)
      const [completed] = await fixtures.readState(bucketId, 'last_result')
      expect(completed.last_result.counters).toMatchObject({
        versionsExamined: 3,
        versionsEligible: 3,
        objectVersionsDeleted: 3,
        batchesCompleted: 2,
      })
      for (const row of rows) await expectBlobAbsent(row.name, row.archived)
    }
  )

  it.each(['promotion', 'foreign key'] as const)(
    'rechecks buffered candidates after a concurrent %s',
    async (change) => {
      await activate()
      const rows = historyRows(['buffer-change/a', 'buffer-change/b'])
      for (const row of rows)
        await seedHistory(row.name, row.archived, row.current, { archivedAt: threeDaysAgo() })
      const target = rows[1]
      const original = helper.adapter.deleteObjectsDetailed.bind(helper.adapter)
      vi.spyOn(helper.adapter, 'deleteObjectsDetailed').mockImplementationOnce(async (...args) => {
        if (change === 'promotion') {
          const transaction = await helper.database.connection.transaction()
          try {
            await transaction.query(
              'UPDATE storage.objects SET archived_at = clock_timestamp() WHERE bucket_id = $1 AND name = $2 AND archived_at IS NULL',
              [bucketId, target.name]
            )
            await transaction.query(
              'UPDATE storage.objects SET archived_at = NULL WHERE bucket_id = $1 AND name = $2 AND version = $3',
              [bucketId, target.name, target.archived]
            )
            await transaction.commit()
          } catch (error) {
            await transaction.rollback()
            throw error
          }
        } else {
          await helper.database.connection.query(
            'CREATE TABLE public.lifecycle_buffer_refs (id uuid REFERENCES storage.objects(id))'
          )
          await helper.database.connection.query(
            'INSERT INTO public.lifecycle_buffer_refs SELECT id FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
            [bucketId, target.name, target.archived]
          )
        }
        return original(...args)
      })
      try {
        await expect(runExecutor({ pageSize: 100, deleteBatchSize: 1 })).resolves.toMatchObject({
          status: change === 'promotion' ? 'INELIGIBLE' : 'COMPLETED',
        })
        await expectBlobAbsent(rows[0].name, rows[0].archived)
        expect(await versions(target.name)).toContainEqual({
          version: target.archived,
          current: change === 'promotion',
        })
        await expectBlobPresent(target.name, target.archived)
      } finally {
        await helper.database.connection.query('DROP TABLE IF EXISTS public.lifecycle_buffer_refs')
      }
    }
  )

  it.each([
    { size: 48, nullVeto: false, statementVeto: false, keepB: false },
    { size: 1, nullVeto: false, statementVeto: false, keepB: true },
    { size: 48, nullVeto: true, statementVeto: false, keepB: false },
    { size: 1, nullVeto: true, statementVeto: false, keepB: true },
    { size: 48, nullVeto: false, statementVeto: true, keepB: true },
  ])(
    'honors state-dependent trigger decisions: batch $size, NULL=$nullVeto, statement=$statementVeto',
    async ({ size, nullVeto, statementVeto, keepB }) => {
      await activate()
      const names = [
        'dependent/B',
        'dependent/A',
        ...Array.from({ length: 47 }, (_, index) => `dependent/F${String(index).padStart(2, '0')}`),
        'dependent/Z',
      ]
      const revisions = new Map<string, string>()
      for (const [index, name] of names.entries()) {
        const version = randomUUID()
        revisions.set(name, version)
        const metadata = await uploadFixture(name, version)
        await helper.database.connection.query(
          `INSERT INTO storage.objects(id, bucket_id, name, version, archived_at, is_versioned, metadata)
         VALUES ($1, $2, $3, $4, '2000-01-01', true, $5)`,
          [
            `00000000-0000-0000-0000-${String(index + 1).padStart(12, '0')}`,
            bucketId,
            name,
            version,
            JSON.stringify(metadata),
          ]
        )
      }
      try {
        await helper.database.connection.query(`
        CREATE TABLE public.lifecycle_dependency_permitted (name text);
        ALTER FUNCTION storage.delete_lifecycle_objects(uuid[]) SET enable_indexscan = off;
        ALTER FUNCTION storage.delete_lifecycle_objects(uuid[]) SET enable_bitmapscan = off;
        ALTER FUNCTION storage.delete_lifecycle_objects(uuid[]) SET enable_indexonlyscan = off;
        CREATE FUNCTION public.lifecycle_dependency_veto() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
        BEGIN
          IF OLD.name = 'dependent/Z' THEN RAISE EXCEPTION 'keep Z' USING ERRCODE = '42501'; END IF;
          IF OLD.name = 'dependent/B' AND NOT EXISTS (
            SELECT 1 FROM storage.objects WHERE bucket_id = OLD.bucket_id AND name = 'dependent/A'
          ) THEN ${nullVeto ? 'RETURN NULL;' : "RAISE EXCEPTION 'keep B without A' USING ERRCODE = '42501';"} END IF;
          IF OLD.name = 'dependent/B' THEN INSERT INTO public.lifecycle_dependency_permitted VALUES (OLD.name); END IF;
          RETURN OLD;
        END $$;
        CREATE TRIGGER lifecycle_dependency_veto BEFORE DELETE ON storage.objects
          FOR EACH ROW EXECUTE FUNCTION public.lifecycle_dependency_veto();
      `)
        if (statementVeto)
          await helper.database.connection.query(`
        CREATE FUNCTION public.lifecycle_dependency_statement() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF (SELECT count(*) FROM removed) > 1 THEN RAISE EXCEPTION 'one row only' USING ERRCODE = '42501'; END IF;
          RETURN NULL;
        END $$;
        CREATE TRIGGER lifecycle_dependency_statement AFTER DELETE ON storage.objects
          REFERENCING OLD TABLE AS removed FOR EACH STATEMENT EXECUTE FUNCTION public.lifecycle_dependency_statement();
      `)
        await expect(runExecutor({ pageSize: 100, deleteBatchSize: size })).resolves.toMatchObject({
          status: 'COMPLETED',
        })
        const permitted = await helper.database.connection.query(
          'SELECT name FROM public.lifecycle_dependency_permitted'
        )
        // Only decisions surviving the actual committed statement authorize bytes.
        // Row fallback visits A before B; bulk visitation is engine/plan dependent.
        if (keepB) expect(permitted.rows).toEqual([])
        const bWasPermitted = permitted.rows.length > 0
        const table = await helper.database.connection.query(
          `SELECT am.amname FROM pg_class AS c JOIN pg_am AS am ON am.oid = c.relam
           WHERE c.oid = 'storage.objects'::regclass`
        )
        if (table.rows[0].amname === 'heap') expect(bWasPermitted).toBe(!keepB)
        for (const name of ['dependent/A', 'dependent/B', 'dependent/Z']) {
          const kept = name === 'dependent/Z' || (name === 'dependent/B' && !bWasPermitted)
          expect(await versions(name)).toHaveLength(kept ? 1 : 0)
          if (kept) await expectBlobPresent(name, revisions.get(name)!)
          else await expectBlobAbsent(name, revisions.get(name)!)
        }
      } finally {
        await helper.database.connection.query(`
        DROP TRIGGER IF EXISTS lifecycle_dependency_veto ON storage.objects;
        DROP TRIGGER IF EXISTS lifecycle_dependency_statement ON storage.objects;
        DROP FUNCTION IF EXISTS public.lifecycle_dependency_veto();
        DROP FUNCTION IF EXISTS public.lifecycle_dependency_statement();
        DROP TABLE IF EXISTS public.lifecycle_dependency_permitted;
        ALTER FUNCTION storage.delete_lifecycle_objects(uuid[]) RESET enable_indexscan;
        ALTER FUNCTION storage.delete_lifecycle_objects(uuid[]) RESET enable_bitmapscan;
        ALTER FUNCTION storage.delete_lifecycle_objects(uuid[]) RESET enable_indexonlyscan;
      `)
      }
    }
  )

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

  async function abandonedKeys() {
    const result = await helper.database.connection.query<{ key: string }>(
      'SELECT key FROM storage.lifecycle_abandoned_artifacts WHERE bucket_id = $1 ORDER BY key',
      [bucketId]
    )
    return result.rows.map((row) => row.key)
  }

  async function abandonedAttempts() {
    const result = await helper.database.connection.query<{ attempted_at: Date }>(
      'SELECT attempted_at FROM storage.lifecycle_abandoned_artifacts WHERE bucket_id = $1',
      [bucketId]
    )
    return result.rows.map((row) => row.attempted_at.getTime())
  }

  async function sweep() {
    await retryAbandonedLifecycleArtifacts({
      storage: helper.storage,
      database: helper.database,
      attemptedBefore: new Date(Date.now() + HOUR_MS).toISOString(),
      budgetMs: 60_000,
    })
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

  async function expectBlobPresent(name: string, version: string | null) {
    await expect(
      helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, version)
    ).resolves.toMatchObject({ size: 1 })
  }

  async function prepareAttempt(name: string, version: string | null, archivedAt: Date) {
    const bucket = await helper.database.findLifecycleBucket(bucketId)
    const generation = bucket.lifecycle_configuration_generation!
    const identity = { bucketId, claimId: randomUUID() }
    expect(
      await helper.database.claimLifecycleBucket({ ...identity, leaseMs: 60_000 })
    ).toBeDefined()
    const { id } = (
      await helper.database.connection.query<{ id: string }>(
        'SELECT id FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version IS NOT DISTINCT FROM $3',
        [bucketId, name, version]
      )
    ).rows[0]
    return {
      ...identity,
      leaseMs: 60_000,
      continuation: newContinuation(generation),
      batch: batchOf([{ id, name, version, isDeleteMarker: false }], {
        name,
        archivedAt: archivedAt.toISOString(),
      }),
    }
  }

  it.each([{ legacy: false }, { legacy: true }])(
    'serializes direct promotion with arming (legacy=$legacy)',
    async ({ legacy }) => {
      await activate()
      const name = 'history/row-lock.txt'
      const archived = legacy ? null : randomUUID()
      const current = randomUUID()
      const archivedAt = threeDaysAgo()
      await seedHistory(name, archived, current, { archivedAt })
      const input = await prepareAttempt(name, archived, archivedAt)
      const writer = new Client({ connectionString: getConfig().databaseURL })
      await writer.connect()
      let arming: Promise<unknown> | undefined
      try {
        await writer.query('BEGIN')
        const pid = (await writer.query('SELECT pg_backend_pid() AS pid')).rows[0].pid
        await writer.query(
          'UPDATE storage.objects SET archived_at = clock_timestamp() WHERE bucket_id = $1 AND name = $2 AND version = $3',
          [bucketId, name, current]
        )
        await writer.query(
          'UPDATE storage.objects SET archived_at = NULL WHERE bucket_id = $1 AND name = $2 AND version IS NOT DISTINCT FROM $3',
          [bucketId, name, archived]
        )
        arming = helper.database.armLifecycleAttempt(input)
        await expect
          .poll(
            async () =>
              (
                await helper.database.connection.query(
                  'SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))) AS blocked',
                  [pid]
                )
              ).rows[0].blocked
          )
          .toBe(true)
        await writer.query('COMMIT')
        await expect(arming).resolves.toEqual({ status: 'ineligible' })
        expect(await versions(name)).toContainEqual({ version: archived, current: true })
        await expectBlobPresent(name, archived)
      } finally {
        await writer.query('ROLLBACK')
        await Promise.allSettled([arming])
        await writer.end()
      }
    }
  )

  it('deduplicates real queue deliveries and re-sends after an expired job fails without queue retries', async () => {
    const { LifecycleBucketRun } = await import('@storage/events/lifecycle-rules/bucket-run')
    await activate()
    const name = 'history/queue-recovery.txt'
    const archived = randomUUID()
    const current = randomUUID()
    const archivedAt = threeDaysAgo()
    await seedHistory(name, archived, current, { archivedAt })
    const schema = `lifecycle_queue_${randomUUID().replaceAll('-', '')}`
    const config = getConfig()
    // pg-boss uses dynamic SQL unsupported by Multigres, use the control DB for this test.
    const queueUrl =
      config.pgQueueConnectionURL ||
      (config.databaseEngine === 'multigres'
        ? config.multitenantDatabaseUrl || config.multitenantDatabasePoolUrl
        : config.databaseURL)
    if (!queueUrl) throw new Error('A PostgreSQL queue database URL is required for this test')
    const queueClient = new Client({ connectionString: queueUrl })
    await queueClient.connect()
    const options = {
      connectionString: queueUrl,
      schema,
      supervise: false,
      schedule: false,
    }
    let boss = new PgBoss(options)
    const payload: LifecycleBucketRunPayload = {
      bucketId,
      tenant: { ref: tenantId, host: '' },
    }
    const queue = LifecycleBucketRun.getQueueOptions()
    try {
      await boss.start()
      await boss.createQueue(queue.name, queue)
      const send = () => boss.send(queue.name, payload, LifecycleBucketRun.getSendOptions(payload))
      const id = await send()
      expect(id).toBeTypeOf('string')
      await expect(send()).resolves.toBeNull()
      const [first] = await boss.fetch<LifecycleBucketRunPayload>(queue.name)
      expect(first.id).toBe(id)
      const input = await prepareAttempt(name, archived, archivedAt)
      expect(await helper.database.armLifecycleAttempt(input)).toMatchObject({ status: 'armed' })
      await expect(runExecutor()).resolves.toMatchObject({ status: 'NOT_CLAIMED' })
      await expect(send()).resolves.toBeNull()
      await boss.stop()

      // Model elapsed lease time without sleeping for the expiry budgets.
      await queueClient.query(
        `UPDATE ${schema}.job SET started_on = '2000-01-01', retry_delay = 0 WHERE id = $1`,
        [id]
      )
      await helper.database.connection.query(
        "UPDATE storage.bucket_lifecycle_states SET claim_until = '2000-01-01', next_run_at = '2000-01-01' WHERE bucket_id = $1",
        [bucketId]
      )
      boss = new PgBoss(options)
      await boss.start()
      await boss.maintain()
      await expect(
        boss.fetch<LifecycleBucketRunPayload>(queue.name, { includeMetadata: true })
      ).resolves.toEqual([])
      const resentId = await send()
      expect(resentId).toBeTypeOf('string')
      expect(resentId).not.toBe(id)
      const [recovered] = await boss.fetch<LifecycleBucketRunPayload>(queue.name)
      expect(recovered.id).toBe(resentId)
      const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')
      await expect(
        new NoncurrentLifecycleExecutor(helper.storage).run(recovered.data.bucketId, {
          tenantLifecycleEnabled: true,
        })
      ).resolves.toMatchObject({ status: 'COMPLETED' })
      await boss.complete(queue.name, recovered.id)
      expect(deletes).not.toHaveBeenCalled()
      expect(await abandonedKeys()).toHaveLength(2)
      expect(await versions(name)).toEqual([{ version: current, current: true }])
    } finally {
      try {
        await boss.stop()
        await queueClient.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`)
      } finally {
        await queueClient.end()
      }
    }
  })

  it('expires history produced by complete uploads and preserves the replacement body', async () => {
    await activate()
    const name = 'history/writer.txt'
    const written: string[] = []
    for (const content of ['previous', 'replacement']) {
      const { obj } = await helper.uploader.upload({
        bucketId,
        objectName: name,
        uploadType: 'standard',
        isUpsert: true,
        file: {
          body: Readable.from(content),
          mimeType: 'text/plain',
          cacheControl: 'no-cache',
          contentLength: Buffer.byteLength(content),
          isTruncated: () => false,
        },
      })
      const version = obj.version as string
      uploaded.push({ name, version })
      written.push(version)
    }
    const [archived, current] = written
    expect(await versions(name)).toEqual([
      { version: archived, current: false },
      { version: current, current: true },
    ])

    await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    await expectBlobAbsent(name, archived)
    const object = await helper.adapter.getObject(
      storageS3Bucket,
      `${tenantId}/${bucketId}/${name}`,
      current
    )
    if (!(object.body instanceof Readable)) throw new Error('Expected a readable object body')
    expect(await text(object.body)).toBe('replacement')
  })

  it('preserves malformed continuation JSON and releases its committed claim after decoding fails', async () => {
    await activate()
    const continuation = { continuationVersion: 999 }
    await helper.database.connection.query(
      'UPDATE storage.bucket_lifecycle_states SET continuation = $2::jsonb WHERE bucket_id = $1',
      [bucketId, JSON.stringify(continuation)]
    )

    const now = new Date()
    for (const [index, delayMinutes] of [5, 10].entries()) {
      await expect(runExecutor({ now: () => now })).rejects.toThrow()
      const rows = await fixtures.readState(
        bucketId,
        'claim_id, claim_until, failure_count, last_error, continuation, next_run_at'
      )
      expect(rows).toEqual([
        expect.objectContaining({
          claim_id: null,
          claim_until: null,
          failure_count: index + 1,
          continuation,
          last_error: expect.objectContaining({ name: 'LifecycleContinuationDecodeError' }),
          next_run_at: new Date(now.getTime() + delayMinutes * 60_000),
        }),
      ])
      await makeDue()
    }
  })

  it('pauses a due bucket of a versioning-disabled bucket without failing', async () => {
    await helper.database.putLifecycleConfiguration(bucketId, configuration)
    const name = 'history/disabled.txt'
    const current = randomUUID()
    await helper.uploader.completeUpload({
      bucketId,
      objectName: name,
      version: current,
      objectMetadata: await uploadFixture(name, current),
      uploadType: 'standard',
    })
    await makeDue()
    const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')

    await expect(runExecutor()).resolves.toMatchObject({ status: 'PAUSED' })
    expect(deletes).not.toHaveBeenCalled()

    const rows = await fixtures.readState(
      bucketId,
      'failure_count, last_error, next_run_at, continuation'
    )
    expect(rows).toEqual([
      { failure_count: 0, last_error: null, next_run_at: null, continuation: null },
    ])
    expect(await versions(name)).toEqual([{ version: current, current: true }])
  })

  it('observes a policy committed while its bucket claim waits for the state row', async () => {
    await fixtures.setStatus(bucketId, 'ENABLED')
    await helper.database.putLifecycleConfiguration(bucketId, {
      rules: [{ ...configuration.rules[0], status: 'Disabled' }],
    })
    await makeDue()

    const writer = await helper.database.connection.transaction()
    const observer = new Client({ connectionString: getConfig().databaseURL })
    await observer.connect()
    let running: ReturnType<typeof runExecutor> | undefined
    try {
      await helper.database.connection.setScope(writer)
      await writer.query(
        `UPDATE storage.buckets SET lifecycle_configuration = $2::jsonb,
           lifecycle_configuration_generation = $3::uuid WHERE id = $1`,
        [bucketId, JSON.stringify(configuration), randomUUID()]
      )
      running = runExecutor()
      await vi.waitFor(async () => {
        const { rows } = await observer.query(`
          SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE wait_event_type = 'Lock'
              AND query LIKE '%UPDATE storage.bucket_lifecycle_states AS state%'
          ) AS blocked
        `)
        expect(rows[0].blocked).toBe(true)
      })
      await writer.commit()

      await expect(running).resolves.toMatchObject({ status: 'COMPLETED' })
      const [state] = await fixtures.readState(bucketId, 'next_run_at, last_result')
      expect(state.next_run_at).not.toBeNull()
      expect(state.last_result).not.toBeNull()
    } finally {
      await writer.rollback().catch(() => undefined)
      await observer.end()
      await running?.catch(() => undefined)
    }
  })

  it('leaves the outbox empty and the backend bytes gone after a successful run', async () => {
    await activate()
    const name = 'history/outbox-success.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current, { archivedAt: threeDaysAgo() })
    const original = helper.adapter.deleteObjectsDetailed.bind(helper.adapter)
    let duringSend: string[] = []
    vi.spyOn(helper.adapter, 'deleteObjectsDetailed').mockImplementationOnce(async (...args) => {
      duringSend = await abandonedKeys()
      return original(...args)
    })

    await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })

    expect(duringSend).toHaveLength(2)
    expect(await abandonedKeys()).toEqual([])
    await expectBlobAbsent(name, archived)
    await expectBlobPresent(name, current)
  })

  it('stops after a rejected backend deletion, keeps its outbox rows and leaves the rest to the sweeper', async () => {
    await activate()
    const rows = ['history/rejected-a.txt', 'history/rejected-b.txt'].map((name, index) => ({
      name,
      archived: randomUUID(),
      current: randomUUID(),
      archivedAt: new Date(Date.now() - (5 - index) * DAY_MS),
    }))
    for (const row of rows) {
      await seedHistory(row.name, row.archived, row.current, { archivedAt: row.archivedAt })
    }
    let armedAttempts: number[] = []
    const deletes = vi
      .spyOn(helper.adapter, 'deleteObjectsDetailed')
      .mockImplementation(async (_bucket, keys) => {
        armedAttempts = await abandonedAttempts()
        return keys.map((key) => ({
          key,
          outcome: 'FAILED' as const,
          error: { code: 'AccessDenied', message: 'deletion denied' },
        }))
      })
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    const now = new Date(Date.now() + 3 * DAY_MS)

    try {
      await expect(
        runExecutor({ pageSize: 100, deleteBatchSize: 1, now: () => now })
      ).resolves.toMatchObject({ status: 'PARTIAL' })
      expect(deletes).toHaveBeenCalledOnce()
      expect(warning).toHaveBeenCalledOnce()
      expect(warning).toHaveBeenCalledWith(
        expect.anything(),
        '[Lifecycle] Backend rejected lifecycle deletions',
        expect.objectContaining({
          metadata: JSON.stringify({ bucketId, failedArtifacts: 2, error: 'deletion denied' }),
        })
      )
    } finally {
      deletes.mockRestore()
      warning.mockRestore()
    }

    const [state] = await fixtures.readState(
      bucketId,
      'failure_count, next_run_at, claim_id, continuation'
    )
    expect(state).toMatchObject({
      failure_count: 1,
      claim_id: null,
      continuation: { counters: { objectVersionsDeleted: 1 } },
    })
    expect(state.next_run_at.getTime()).toBeLessThanOrEqual(now.getTime() + 15 * 60_000)
    expect(state.continuation).not.toHaveProperty('batch')
    expect(await versions(rows[0].name)).toEqual([{ version: rows[0].current, current: true }])
    expect(await versions(rows[1].name)).toHaveLength(2)
    expect(await abandonedKeys()).toHaveLength(2)
    expect(Math.min(...(await abandonedAttempts()))).toBeGreaterThan(Math.max(...armedAttempts))
    await expectBlobPresent(rows[0].name, rows[0].archived)

    await sweep()
    expect(await abandonedKeys()).toEqual([])
    await expectBlobAbsent(rows[0].name, rows[0].archived)
  })

  it('keeps the outbox rows and does not block bucket deletion when the send throws after arm', async () => {
    await activate()
    const name = 'history/crash-after-arm.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current, { archivedAt: threeDaysAgo() })
    const failure = new Error('connection reset')
    vi.spyOn(helper.adapter, 'deleteObjectsDetailed').mockRejectedValueOnce(failure)
    const now = new Date()

    await expect(runExecutor({ now: () => now })).rejects.toBe(failure)

    const [state] = await fixtures.readState(
      bucketId,
      'failure_count, next_run_at, claim_id, continuation'
    )
    expect(state).toMatchObject({
      failure_count: 1,
      claim_id: null,
      next_run_at: new Date(now.getTime() + 5 * 60_000),
      continuation: { counters: { objectVersionsDeleted: 1 } },
    })
    expect(state.continuation).not.toHaveProperty('batch')
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    expect(await abandonedKeys()).toHaveLength(2)
    await expectBlobPresent(name, archived)

    await expect(
      withDeleteEnabled(helper.database.connection, async (transaction) => {
        await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
        return transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
      })
    ).resolves.toMatchObject({ rowCount: 1 })
    expect(await abandonedKeys()).toHaveLength(2)

    await sweep()
    expect(await abandonedKeys()).toEqual([])
    await expectBlobAbsent(name, archived)
  })

  it('prevents new foreign-key references after metadata is deleted and before backend deletion', async () => {
    await activate()
    const name = 'history/late-reference.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current, { archivedAt: threeDaysAgo() })
    const rows = await helper.database.connection.query<{ id: string }>(
      'SELECT id FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
      [bucketId, name, archived]
    )
    await helper.database.connection.query(
      'CREATE TABLE public.lifecycle_late_ref (object_id uuid REFERENCES storage.objects(id))'
    )
    const original = helper.adapter.deleteObjectsDetailed.bind(helper.adapter)
    const deletion = vi
      .spyOn(helper.adapter, 'deleteObjectsDetailed')
      .mockImplementation(async (root, keys, options) => {
        await expect(
          helper.database.connection.query('INSERT INTO public.lifecycle_late_ref VALUES ($1)', [
            rows.rows[0].id,
          ])
        ).rejects.toMatchObject({ code: '23503' })
        return original(root, keys, options)
      })
    try {
      await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
      expect(deletion).toHaveBeenCalledOnce()
      await expectBlobAbsent(name, archived)
    } finally {
      await helper.database.connection.query('DROP TABLE public.lifecycle_late_ref')
    }
  })

  function longName(length: number) {
    return Array.from({ length }, (_, index) => (index % 200 === 199 ? '/' : 'x')).join('')
  }

  it.skipIf(storageBackendType !== 's3')(
    'expires a version whose .info sibling exceeds the S3 key limit and releases the promotion fence',
    async () => {
      await activate()
      const archived = randomUUID()
      const current = randomUUID()
      const prefix = `${tenantId}/${bucketId}/`
      // The data key is 1020 bytes, so its .info sibling is 1025 and cannot exist in S3.
      const name = longName(1020 - prefix.length - 37)
      await seedHistory(name, archived, current, { archivedAt: threeDaysAgo() })
      const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')

      await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
      for (const [, keys] of deletes.mock.calls) {
        expect(keys.every((key) => Buffer.byteLength(key) <= 1024)).toBe(true)
      }

      const [state] = await fixtures.readState(bucketId, 'continuation, failure_count, last_error')
      expect(state).toEqual({ continuation: null, failure_count: 0, last_error: null })
      expect(await versions(name)).toEqual([{ version: current, current: true }])
      await expectBlobAbsent(name, archived)
      await helper.storage.from(bucketId).deleteObject(name, current)
      expect(await versions(name)).toEqual([])
    }
  )

  it('expires an archived delete marker without a backend call', async () => {
    await activate()
    const name = 'history/marker.txt'
    const version = randomUUID()
    await helper.database.connection.query(
      `INSERT INTO storage.objects (bucket_id, name, version, is_versioned, is_delete_marker, metadata, archived_at)
       VALUES ($1, $2, $3, true, true, NULL, $4)`,
      [bucketId, name, version, threeDaysAgo()]
    )
    const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')

    await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(deletes).not.toHaveBeenCalled()

    const [state] = await fixtures.readState(bucketId, 'continuation, last_result')
    expect(state.continuation).toBeNull()
    expect(state.last_result.counters).toMatchObject({
      deleteMarkersDeleted: 1,
      objectVersionsDeleted: 0,
    })
    expect(await versions(name)).toEqual([])
  })

  it('loses its idle claim when direct SQL removes the policy between evaluation and arm', async () => {
    await activate()
    const name = 'history/config-removed.txt'
    const archived = randomUUID()
    await seedHistory(name, archived, randomUUID(), {
      archivedAt: threeDaysAgo(),
    })
    const original = StoragePgDB.prototype.armLifecycleAttempt
    vi.spyOn(StoragePgDB.prototype, 'armLifecycleAttempt').mockImplementationOnce(async function (
      this: StoragePgDB,
      ...args
    ) {
      await fixtures.withServiceOperation('storage.s3.bucket.put_lifecycle', (transaction) =>
        transaction.query(
          'UPDATE storage.buckets SET lifecycle_configuration = NULL, lifecycle_configuration_generation = NULL WHERE id = $1',
          [bucketId]
        )
      )
      return original.apply(this, args)
    })
    const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')

    await expect(runExecutor()).resolves.toMatchObject({ status: 'LOST_CLAIM' })
    expect(deletes).not.toHaveBeenCalled()
    await expect(fixtures.readState(bucketId, '1')).resolves.toEqual([])
  })

  it.each(['findLifecycleBucket', 'armLifecycleAttempt'] as const)(
    'loses its claim without an error when the bucket is deleted before %s',
    async (method) => {
      await activate()
      await helper.database.connection.query(
        `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned, is_delete_marker)
         VALUES ($1, 'gone/a', gen_random_uuid()::text, '2000-01-01', true, true)`,
        [bucketId]
      )
      const original = StoragePgDB.prototype[method] as (...args: unknown[]) => Promise<unknown>
      vi.spyOn(StoragePgDB.prototype, method).mockImplementationOnce(async function (
        this: StoragePgDB,
        ...args: unknown[]
      ) {
        await withDeleteEnabled(helper.database.connection, async (transaction) => {
          await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
          await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
        })
        return original.apply(this, args) as never
      })

      await expect(runExecutor()).resolves.toEqual({ status: 'LOST_CLAIM' })
      await expect(fixtures.readState(bucketId, '1')).resolves.toEqual([])
    }
  )

  it('leaves fresh history and its continuation untouched under a closed tenant gate', async () => {
    await activate()
    const name = 'history/paused.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current)
    const before = await versions(name)

    await expect(runExecutor({ tenantLifecycleEnabled: false })).resolves.toMatchObject({
      status: 'PAUSED',
    })

    expect(await versions(name)).toEqual(before)
    for (const version of [archived, current]) {
      await expectBlobPresent(name, version)
    }
    const rows = await fixtures.readState(bucketId, 'continuation, claim_id, next_run_at')
    expect(rows).toEqual([{ continuation: null, claim_id: null, next_run_at: expect.any(Date) }])
  })

  it.each([
    {
      label: 'enabled history',
      status: 'ENABLED',
      legacyNull: false,
      isVersioned: true,
      realClock: false,
    },
    {
      label: 'suspended history',
      status: 'SUSPENDED',
      legacyNull: false,
      isVersioned: true,
      realClock: false,
    },
    {
      label: 'legacy physical null version',
      status: 'ENABLED',
      legacyNull: true,
      isVersioned: false,
      realClock: false,
    },
    {
      label: 'logical null version under a physical UUID',
      status: 'ENABLED',
      legacyNull: false,
      isVersioned: false,
      realClock: false,
    },
    {
      label: 'two-day-old history at the real clock',
      status: 'ENABLED',
      legacyNull: false,
      isVersioned: true,
      realClock: true,
    },
  ] as const)(
    'expires $label and preserves current bytes and sidecar-like siblings',
    async ({ status, legacyNull, isVersioned, realClock }) => {
      const name = 'history/expiry.txt'
      const archived = legacyNull ? null : randomUUID()
      const current = randomUUID()
      await seedHistory(name, archived, current, {
        isVersioned,
        ...(realClock && { archivedAt: new Date(Date.now() - 2 * DAY_MS) }),
      })
      if (legacyNull && storageBackendType === 'file') {
        // Model missing legacy bytes followed by a real replacement upload.
        await helper.adapter.deleteObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, null)
        await uploadFixture(name, current)
      }
      await activate(status)
      if (legacyNull && storageBackendType === 's3') await uploadFixture(`${name}.info`, null)
      expect(await versions(name)).toContainEqual({ version: archived, current: false })

      await expect(runExecutor(realClock ? { now: () => new Date() } : {})).resolves.toMatchObject({
        status: 'COMPLETED',
      })
      expect(await versions(name)).toEqual([{ version: current, current: true }])
      await expectBlobAbsent(name, archived)
      await expectBlobPresent(name, current)
      const currentObject = await helper.adapter.getObject(
        storageS3Bucket,
        `${tenantId}/${bucketId}/${name}`,
        current
      )
      if (!(currentObject.body instanceof Readable))
        throw new Error('Expected a readable object body')
      expect(await text(currentObject.body)).toBe('x')
      if (legacyNull && storageBackendType === 's3') {
        await expect(
          helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}.info`, null)
        ).resolves.toMatchObject({ size: 1 })
      }
    }
  )

  it('expires an intact legacy file archived by a delete marker', async () => {
    const name = 'history/legacy.txt'
    const metadata = await uploadFixture(name, null)
    await helper.database.connection.query(
      `INSERT INTO storage.objects (bucket_id, name, version, is_versioned, metadata)
       VALUES ($1, $2, NULL, false, $3::jsonb)`,
      [bucketId, name, JSON.stringify(metadata)]
    )
    await activate()
    await helper.storage.from(bucketId).deleteObject(name)
    const rows = await versions(name)
    const marker = rows.find((row) => row.current)!
    expect(marker.version).toEqual(expect.any(String))
    expect(rows).toContainEqual({ version: null, current: false })
    await expectBlobPresent(name, null)

    await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(await versions(name)).toEqual([marker])
    await expectBlobAbsent(name, null)
  })

  it.each([
    { label: 'null metadata', metadata: null, bytes: 0 },
    { label: 'numeric string size', metadata: { size: '1' }, bytes: 1 },
  ])('continues past $label and preserves byte accounting', async ({ metadata, bytes }) => {
    await activate()
    const archivedAt = threeDaysAgo()
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
    await expect(runExecutor({ pageSize: 1 })).resolves.toMatchObject({ status: 'COMPLETED' })
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

    const [result] = await fixtures.readState(bucketId, 'continuation, last_result')
    expect(result).toMatchObject({
      continuation: null,
      last_result: { counters: { objectVersionsDeleted: 2, bytesDeleted: bytes + 1 } },
    })
    for (const row of [first, second]) {
      expect(await versions(row.name)).toEqual([{ version: row.current, current: true }])
      await expectBlobAbsent(row.name, row.archived)
      await expectBlobPresent(row.name, row.current)
    }
  })

  it.each([1, 2])('re-reads unstored work after a failure before arm %i', async (failAt) => {
    await activate()
    const archivedAt = threeDaysAgo()
    const rows = historyRows(['retry/a', 'retry/b'])
    for (const row of rows) await seedHistory(row.name, row.archived, row.current, { archivedAt })
    const now = new Date()
    const failure = new Error('worker failed before arming')
    const arm = StoragePgDB.prototype.armLifecycleAttempt
    const attempts = new Set<unknown>()
    const arming = vi
      .spyOn(StoragePgDB.prototype, 'armLifecycleAttempt')
      .mockImplementation(async function (this: StoragePgDB, input) {
        // The public method delegates into a transaction with the same input.
        attempts.add(input.batch)
        if (attempts.size === failAt) throw failure
        return arm.call(this, input)
      })
    await expect(runExecutor({ pageSize: 100, deleteBatchSize: 1, now: () => now })).rejects.toBe(
      failure
    )
    arming.mockRestore()
    const [state] = await fixtures.readState(bucketId, 'continuation')
    if (failAt === 1) expect(state.continuation).toBeNull()
    else {
      expect(state.continuation).toMatchObject({
        snapshotAt: now.toISOString(),
        cursor: { name: rows[0].name },
        counters: { objectVersionsDeleted: 1 },
      })
    }
    await makeDue()
    const retryNow = new Date(now.getTime() + 15 * 60_000)
    await expect(
      runExecutor({ pageSize: 100, deleteBatchSize: 1, now: () => retryNow })
    ).resolves.toMatchObject({
      status: 'COMPLETED',
    })
    const [completed] = await fixtures.readState(bucketId, 'last_result')
    expect(completed.last_result).toMatchObject({
      snapshotAt: (failAt === 1 ? retryNow : now).toISOString(),
      counters: {
        versionsExamined: 2,
        versionsEligible: 2,
        objectVersionsDeleted: 2,
        batchesCompleted: 2,
      },
    })
    for (const row of rows) {
      expect(await versions(row.name)).toEqual([{ version: row.current, current: true }])
      await expectBlobAbsent(row.name, row.archived)
    }
  })

  it('keeps the outbox rows when arm commits but its acknowledgement is lost', async () => {
    await activate()
    const name = 'history/lost-ack.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current, { archivedAt: threeDaysAgo() })
    const original = StoragePgDB.prototype.armLifecycleAttempt
    const lost = new Error('acknowledgement lost')
    vi.spyOn(StoragePgDB.prototype, 'armLifecycleAttempt').mockImplementationOnce(async function (
      this: StoragePgDB,
      input
    ) {
      await original.call(this, input)
      throw lost
    })
    const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')
    const now = new Date()
    await expect(runExecutor({ now: () => now })).rejects.toBe(lost)
    expect(deletes).not.toHaveBeenCalled()
    const [state] = await fixtures.readState(
      bucketId,
      'claim_id, failure_count, next_run_at, continuation'
    )
    expect(state).toMatchObject({
      claim_id: null,
      failure_count: 1,
      next_run_at: new Date(now.getTime() + 5 * 60_000),
    })
    expect(state.continuation).not.toHaveProperty('batch')
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    expect(await abandonedKeys()).toHaveLength(2)

    await makeDue()
    await expect(
      runExecutor({ now: () => new Date(now.getTime() + 5 * 60_000) })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    await sweep()
    expect(await abandonedKeys()).toEqual([])
    await expectBlobAbsent(name, archived)
  })

  it('keeps the committed cursor and counters when an evaluated row is deleted before arming', async () => {
    await activate()
    const archivedAt = threeDaysAgo()
    const rows = historyRows(['resume/a', 'resume/b', 'resume/c'])
    for (const row of rows) await seedHistory(row.name, row.archived, row.current, { archivedAt })
    const arm = StoragePgDB.prototype.armLifecycleAttempt
    const attempts = new Set<unknown>()
    const arming = vi
      .spyOn(StoragePgDB.prototype, 'armLifecycleAttempt')
      .mockImplementation(async function (this: StoragePgDB, input) {
        if (attempts.size === 1 && !attempts.has(input.batch)) {
          await helper.storage.from(bucketId).deleteObject(rows[1].name, rows[1].archived)
        }
        attempts.add(input.batch)
        return arm.call(this, input)
      })
    try {
      await expect(runExecutor({ pageSize: 1 })).resolves.toMatchObject({ status: 'INELIGIBLE' })
    } finally {
      arming.mockRestore()
    }
    const [state] = await fixtures.readState(bucketId, 'continuation, claim_id, failure_count')
    expect(state).toMatchObject({
      claim_id: null,
      failure_count: 0,
      continuation: { cursor: { name: rows[0].name }, counters: { objectVersionsDeleted: 1 } },
    })

    await expect(runExecutor({ pageSize: 1 })).resolves.toMatchObject({ status: 'COMPLETED' })
    const [completed] = await fixtures.readState(bucketId, 'last_result')
    expect(completed.last_result.counters.objectVersionsDeleted).toBe(2)
    for (const row of rows) {
      expect(await versions(row.name)).toEqual([{ version: row.current, current: true }])
    }
  })

  it('persists the scanned cursor when arming finds a candidate ineligible', async () => {
    await activate()
    await helper.database.putLifecycleConfiguration(bucketId, {
      rules: [
        {
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 1, newerNoncurrentVersions: 1 },
        },
      ],
    })
    await helper.database.connection.query(
      `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned, is_delete_marker)
       SELECT $1, 'keep/' || lpad(n::text, 3, '0'), gen_random_uuid()::text, '2000-01-01'::timestamptz, true, true
       FROM generate_series(1, 10) AS n`,
      [bucketId]
    )
    const newer = randomUUID()
    await helper.database.connection.query(
      `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned, is_delete_marker)
       VALUES ($1, 'zcand', gen_random_uuid()::text, '2000-01-02', true, true),
              ($1, 'zcand', $2, '2000-01-03', true, true)`,
      [bucketId, newer]
    )
    const evaluation = vi.spyOn(StoragePgDB.prototype, 'evaluateNoncurrentLifecyclePage')
    const arm = StoragePgDB.prototype.armLifecycleAttempt
    vi.spyOn(StoragePgDB.prototype, 'armLifecycleAttempt').mockImplementationOnce(async function (
      this: StoragePgDB,
      input
    ) {
      await withDeleteEnabled(helper.database.connection, (tx) =>
        tx.query('DELETE FROM storage.objects WHERE bucket_id = $1 AND version = $2', [
          bucketId,
          newer,
        ])
      )
      return arm.call(this, input)
    })

    await expect(runExecutor({ pageSize: 2 })).resolves.toMatchObject({ status: 'INELIGIBLE' })
    const firstRunPages = evaluation.mock.calls.length
    const [state] = await fixtures.readState(
      bucketId,
      'claim_id, next_run_at <= clock_timestamp() AS due, continuation'
    )
    expect(state).toMatchObject({ claim_id: null, due: true })
    expect(state.continuation?.cursor?.name).toBe('keep/010')

    evaluation.mockClear()
    await expect(runExecutor({ pageSize: 2 })).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(evaluation.mock.calls.length).toBeLessThan(firstRunPages)
  })

  const foreignKey = (options = '') => ({
    install: async (ids: string[]) => {
      await helper.database.connection.query(
        `CREATE TABLE public.lifecycle_probe_refs (object_id uuid REFERENCES storage.objects(id) ${options})`
      )
      await helper.database.connection.query(
        'INSERT INTO public.lifecycle_probe_refs VALUES ($1)',
        [ids[0]]
      )
    },
    uninstall: () =>
      helper.database.connection.query('DROP TABLE IF EXISTS public.lifecycle_probe_refs'),
  })
  const keepTrigger = (body: (ids: string[]) => string) => ({
    install: async (ids: string[]) => {
      await helper.database.connection.query(
        `CREATE FUNCTION public.lifecycle_probe_keep() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN ${body(ids)} RETURN OLD; END $$`
      )
      await helper.database.connection.query(
        `CREATE TRIGGER lifecycle_probe_keep BEFORE DELETE ON storage.objects
         FOR EACH ROW EXECUTE FUNCTION public.lifecycle_probe_keep()`
      )
    },
    uninstall: async () => {
      await helper.database.connection.query(
        'DROP TRIGGER IF EXISTS lifecycle_probe_keep ON storage.objects'
      )
      await helper.database.connection.query(
        'DROP FUNCTION IF EXISTS public.lifecycle_probe_keep()'
      )
    },
  })

  it.each<{
    veto: string
    kept: number
    pageSize?: number
    error?: string
    install: (ids: string[]) => Promise<unknown>
    uninstall: () => Promise<unknown>
  }>([
    { veto: 'a foreign key', kept: 1, ...foreignKey() },
    { veto: 'a foreign key on the oldest page', kept: 1, pageSize: 1, ...foreignKey() },
    {
      veto: 'a deferred foreign key',
      kept: 1,
      ...foreignKey('DEFERRABLE INITIALLY DEFERRED'),
    },
    {
      veto: 'a BEFORE DELETE trigger returning NULL for one row of the batch',
      kept: 1,
      ...keepTrigger((ids) => `IF OLD.id = '${ids[0]}' THEN RETURN NULL; END IF;`),
    },
    {
      veto: 'a BEFORE DELETE trigger returning NULL',
      kept: 2,
      ...keepTrigger(() => `IF OLD.bucket_id = '${bucketId}' THEN RETURN NULL; END IF;`),
    },
    {
      veto: 'a BEFORE DELETE trigger raising a transient error',
      kept: 2,
      error: 'ResourceLocked',
      ...keepTrigger(
        (ids) =>
          `IF OLD.id = '${ids[0]}' THEN RAISE EXCEPTION 'busy' USING ERRCODE = '55P03'; END IF;`
      ),
    },
  ])(
    'keeps the bytes of versions held by $veto and deletes the rest',
    async ({ kept, pageSize = 100, error, install, uninstall }) => {
      await activate()
      const rows = ['veto/a.txt', 'veto/b.txt'].map((name) => ({
        name,
        archived: randomUUID(),
      }))
      for (const row of rows) {
        await seedHistory(row.name, row.archived, randomUUID(), {
          archivedAt: threeDaysAgo(),
        })
      }
      const ids = (
        await helper.database.connection.query<{ id: string }>(
          'SELECT id FROM storage.objects WHERE bucket_id = $1 AND name = $2 AND version = $3',
          [bucketId, rows[0].name, rows[0].archived]
        )
      ).rows.map((row) => row.id)

      try {
        await install(ids)
        const run = runExecutor({ pageSize })
        if (error) await expect(run).rejects.toMatchObject({ code: error })
        else await expect(run).resolves.toMatchObject({ status: 'COMPLETED' })
        for (const row of rows.slice(0, kept)) {
          await expectBlobPresent(row.name, row.archived)
          expect(await versions(row.name)).toHaveLength(2)
        }
        for (const row of rows.slice(kept)) {
          expect(await versions(row.name)).toEqual([{ version: expect.any(String), current: true }])
          await expectBlobAbsent(row.name, row.archived)
        }
        const [state] = await fixtures.readState(
          bucketId,
          'failure_count, continuation, last_error, last_result'
        )
        expect(state).toMatchObject({ failure_count: error ? 1 : 0 })
        expect(state.continuation?.batch).toBeUndefined()
        if (!error) {
          expect(state.last_result.counters.objectVersionsDeleted).toBe(rows.length - kept)
        }
        expect(Boolean(state.last_error)).toBe(Boolean(error))
      } finally {
        await uninstall()
      }

      await makeDue()
      await expect(
        runExecutor({ now: () => new Date(Date.now() + 4 * DAY_MS) })
      ).resolves.toMatchObject({ status: 'COMPLETED' })
      for (const row of rows) {
        expect(await versions(row.name)).toEqual([{ version: expect.any(String), current: true }])
        await expectBlobAbsent(row.name, row.archived)
      }
    }
  )

  it('finishes a 100-row page in bounded delete transactions without overflowing on vetoes', async (context) => {
    const server = await helper.database.connection.query<{ version: string }>(
      `SELECT current_setting('server_version_num') AS version`
    )
    if (Number(server.rows[0].version) < 160000) context.skip()
    await activate()
    await helper.database.connection.query(
      `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned, is_delete_marker)
       SELECT $1, 'bounded/' || lpad(n::text, 3, '0'), gen_random_uuid()::text,
              '2000-01-01'::timestamptz, true, true
       FROM generate_series(1, 100) AS n`,
      [bucketId]
    )
    try {
      await helper.database.connection.query(`
        CREATE TABLE public.lifecycle_batch_audit (transaction_id text, overflowed boolean);
        CREATE FUNCTION public.lifecycle_batch_veto() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.name = 'bounded/001' THEN
            RAISE EXCEPTION 'keep this marker' USING ERRCODE = '42501';
          END IF;
          RETURN OLD;
        END $$;
        CREATE TRIGGER lifecycle_batch_veto BEFORE DELETE ON storage.objects
          FOR EACH ROW EXECUTE FUNCTION public.lifecycle_batch_veto();
        CREATE FUNCTION public.lifecycle_batch_audit_fn() RETURNS trigger
          LANGUAGE plpgsql SECURITY DEFINER AS $$
        BEGIN
          IF (SELECT count(*) FROM removed) > 1 THEN
            RAISE EXCEPTION 'delete rows individually' USING ERRCODE = '42501';
          END IF;
          PERFORM pg_stat_clear_snapshot();
          INSERT INTO public.lifecycle_batch_audit
          SELECT pg_current_xact_id()::text, subxact.subxact_overflowed
          FROM pg_stat_get_backend_idset() AS backend,
            LATERAL pg_stat_get_backend_subxact(backend) AS subxact
          WHERE pg_stat_get_backend_pid(backend) = pg_backend_pid();
          RETURN NULL;
        END $$;
        CREATE TRIGGER lifecycle_batch_audit AFTER DELETE ON storage.objects
          REFERENCING OLD TABLE AS removed FOR EACH STATEMENT
          EXECUTE FUNCTION public.lifecycle_batch_audit_fn();
      `)

      await expect(runExecutor({ pageSize: 96 })).resolves.toMatchObject({ status: 'COMPLETED' })
      const audit = await helper.database.connection.query(
        `SELECT count(*)::integer AS deleted, bool_or(overflowed) AS overflowed
         FROM public.lifecycle_batch_audit GROUP BY transaction_id ORDER BY deleted`
      )
      expect(audit.rows).toEqual([
        { deleted: 4, overflowed: false },
        { deleted: 47, overflowed: false },
        { deleted: 48, overflowed: false },
      ])
      const remaining = await helper.database.connection.query(
        'SELECT name FROM storage.objects WHERE bucket_id = $1',
        [bucketId]
      )
      expect(remaining.rows).toEqual([{ name: 'bounded/001' }])
      const [state] = await fixtures.readState(bucketId, 'continuation, last_result')
      expect(state.continuation).toBeNull()
      expect(state.last_result.counters).toMatchObject({
        versionsExamined: 100,
        versionsEligible: 100,
        deleteMarkersDeleted: 99,
        batchesCompleted: 3,
      })
    } finally {
      await helper.database.connection.query(`
        DROP TRIGGER IF EXISTS lifecycle_batch_veto ON storage.objects;
        DROP TRIGGER IF EXISTS lifecycle_batch_audit ON storage.objects;
        DROP FUNCTION IF EXISTS public.lifecycle_batch_veto();
        DROP FUNCTION IF EXISTS public.lifecycle_batch_audit_fn();
        DROP TABLE IF EXISTS public.lifecycle_batch_audit;
      `)
    }
  })

  it('counts a version once when a second worker takes over during the first worker delete', async () => {
    await activate()
    const name = 'history/lease-race.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current, { archivedAt: threeDaysAgo() })
    const deleteObjects = helper.adapter.deleteObjectsDetailed.bind(helper.adapter)
    let second: unknown
    vi.spyOn(helper.adapter, 'deleteObjectsDetailed').mockImplementationOnce(
      async (bucket, keys, options) => {
        await helper.database.connection.query(
          "UPDATE storage.bucket_lifecycle_states SET claim_until = '2000-01-01' WHERE bucket_id = $1",
          [bucketId]
        )
        second = await runExecutor({ now: () => new Date(Date.now() + 3 * DAY_MS + 60_000) })
        return deleteObjects(bucket, keys, options)
      }
    )

    await expect(runExecutor()).resolves.toMatchObject({ status: 'LOST_CLAIM' })

    expect(second).toMatchObject({ status: 'COMPLETED' })
    const [state] = await fixtures.readState(bucketId, 'continuation, last_result')
    expect(state.continuation).toBeNull()
    expect(state.last_result.counters.objectVersionsDeleted).toBe(1)
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    await expectBlobAbsent(name, archived)
  })

  it('loses its claim on the next arm when a policy write lands during a batch, then finishes under the new generation', async () => {
    await activate()
    const names = ['history/policy-a.txt', 'history/policy-b.txt']
    const archived = names.map(() => randomUUID())
    for (const [index, name] of names.entries()) {
      await seedHistory(name, archived[index], randomUUID(), {
        archivedAt: new Date(Date.now() - (4 - index) * DAY_MS),
      })
    }
    const deleteObjects = helper.adapter.deleteObjectsDetailed.bind(helper.adapter)
    vi.spyOn(helper.adapter, 'deleteObjectsDetailed').mockImplementationOnce(
      async (bucket, keys, options) => {
        await helper.database.putLifecycleConfiguration(bucketId, {
          rules: [
            { ...configuration.rules[0], noncurrentVersionExpiration: { noncurrentDays: 2 } },
          ],
        })
        return deleteObjects(bucket, keys, options)
      }
    )

    await expect(runExecutor({ pageSize: 100, deleteBatchSize: 1 })).resolves.toMatchObject({
      status: 'LOST_CLAIM',
    })

    const [state] = await fixtures.readState(bucketId, 'continuation, failure_count')
    expect(state).toEqual({ continuation: null, failure_count: 0 })
    expect(await versions(names[0])).toHaveLength(1)
    await expectBlobAbsent(names[0], archived[0])
    expect(await versions(names[1])).toHaveLength(2)

    await makeDue()
    await expect(runExecutor({ pageSize: 100, deleteBatchSize: 1 })).resolves.toMatchObject({
      status: 'COMPLETED',
    })
    expect(await versions(names[1])).toHaveLength(1)
    await expectBlobAbsent(names[1], archived[1])
  })

  it('keeps the blob of an abandoned record whose version belongs to a live object', async () => {
    const name = 'history/reused.txt'
    const version = randomUUID()
    await uploadFixture(name, version)
    await helper.database.connection.query(
      `INSERT INTO storage.objects (bucket_id, name, version, is_versioned, metadata)
       VALUES ($1, $2, $3, true, '{"size":1}'::jsonb)`,
      [bucketId, name, version]
    )
    await helper.database.connection.query(
      `INSERT INTO storage.lifecycle_abandoned_artifacts (bucket_id, key, name, version)
       VALUES ($1, $2, $3, $4)`,
      [bucketId, withOptionalVersion(name, version), name, version]
    )
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})

    await sweep()

    expect(await abandonedKeys()).toEqual([])
    await expectBlobPresent(name, version)
    expect(warning).toHaveBeenCalledOnce()
  })
})
