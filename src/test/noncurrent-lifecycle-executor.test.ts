import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import { text } from 'node:stream/consumers'
import { logSchema } from '@internal/monitoring'
import { isMissingBackendObject, withOptionalVersion } from '@storage/backend'
import { StoragePgDB } from '@storage/database'
import type { LifecycleBucketShardRunPayload } from '@storage/events/lifecycle-rules/bucket-shard-run'
import {
  type LifecycleExecutorOptions,
  NoncurrentLifecycleExecutor,
} from '@storage/lifecycle/executor'
import { Client } from 'pg'
import PgBoss from 'pg-boss'
import { getConfig } from '../config'
import { armedContinuation } from './utils/lifecycle-continuation'
import { shard, useLifecycleStorage } from './utils/lifecycle-versioning'

const { storageBackendType, storageS3Bucket, tenantId } = getConfig()
const DAY_MS = 24 * 60 * 60 * 1000

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
    // Only move time for expiry/recovery cases; normal cases keep the writer's time.
    if (options.archivedAt) {
      await helper.database.connection.query(
        `UPDATE storage.objects SET archived_at = $4
         WHERE bucket_id = $1 AND name = $2 AND version IS NOT DISTINCT FROM $3`,
        [bucketId, name, archivedVersion, options.archivedAt]
      )
    }
  }

  async function activate(status: 'ENABLED' | 'SUSPENDED' = 'ENABLED') {
    await fixtures.setStatus(bucketId, status)
    await helper.database.putLifecycleConfiguration(bucketId, configuration)
  }

  function runExecutor(
    options: Partial<LifecycleExecutorOptions> & { tenantLifecycleEnabled?: boolean } = {}
  ) {
    const { tenantLifecycleEnabled = true, ...executorOptions } = options
    return new NoncurrentLifecycleExecutor(helper.storage, {
      now: () => new Date(Date.now() + 3 * DAY_MS),
      ...executorOptions,
    }).run(shard(bucketId), { tenantLifecycleEnabled })
  }

  const makeDue = () =>
    helper.database.connection.query(
      "UPDATE storage.bucket_lifecycle_states SET next_run_at = '2000-01-01' WHERE bucket_id = $1",
      [bucketId]
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

  async function prepareAttempt(name: string, version: string | null, archivedAt: Date) {
    const bucket = await helper.database.findLifecycleBucket(bucketId)
    const generation = bucket.lifecycle_configuration_generation!
    const identity = { ...shard(bucketId), claimId: randomUUID() }
    expect(
      await helper.database.claimLifecycleShard({ ...identity, leaseMs: 60_000 })
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
      continuation: armedContinuation(
        generation,
        [{ id, name, version, isDeleteMarker: false }],
        { name, archivedAt: archivedAt.toISOString() },
        { startedAt: new Date(Date.now() - DAY_MS).toISOString() }
      ),
    }
  }

  it.each([
    { armFirst: true, legacy: false },
    { armFirst: false, legacy: false },
    { armFirst: true, legacy: true },
    { armFirst: false, legacy: true },
  ])('serializes direct promotion with arming (armFirst=$armFirst, legacy=$legacy)', async ({
    armFirst,
    legacy,
  }) => {
    await activate()
    const name = 'history/row-lock.txt'
    const archived = legacy ? null : randomUUID()
    const current = randomUUID()
    const archivedAt = new Date(Date.now() - 3 * DAY_MS)
    await seedHistory(name, archived, current, { archivedAt })
    const input = await prepareAttempt(name, archived, archivedAt)
    const writer = new Client({ connectionString: getConfig().databaseURL })
    await writer.connect()
    let promotion: Promise<unknown> | undefined
    let arming: Promise<unknown> | undefined
    try {
      await writer.query('BEGIN')
      const pid = (await writer.query('SELECT pg_backend_pid() AS pid')).rows[0].pid
      await writer.query(
        'UPDATE storage.objects SET archived_at = clock_timestamp() WHERE bucket_id = $1 AND name = $2 AND version = $3',
        [bucketId, name, current]
      )
      const promote = () =>
        writer.query(
          'UPDATE storage.objects SET archived_at = NULL WHERE bucket_id = $1 AND name = $2 AND version IS NOT DISTINCT FROM $3',
          [bucketId, name, archived]
        )
      if (armFirst) {
        await helper.database.withTransaction(async (db) => {
          expect(await db.armLifecycleAttempt(input)).toBeDefined()
          promotion = promote().then(
            () => ({ promoted: true }),
            (error: unknown) => error
          )
          await expect
            .poll(
              async () =>
                (
                  await helper.database.connection.query(
                    'SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked',
                    [pid]
                  )
                ).rows[0].blocked
            )
            .toBe(true)
        })
        // The trigger must see the journal committed after the UPDATE began.
        await expect(promotion).resolves.toMatchObject({ code: '55P03' })
        await writer.query('ROLLBACK')
        expect(await versions(name)).toContainEqual({ version: current, current: true })
      } else {
        await promote()
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
        await expect(arming).resolves.toBeUndefined()
        expect(await versions(name)).toContainEqual({ version: archived, current: true })
        await expect(
          helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, archived)
        ).resolves.toMatchObject({ size: 1 })
      }
    } finally {
      await writer.query('ROLLBACK')
      await Promise.allSettled([promotion, arming])
      await writer.end()
    }
  })

  it('deduplicates real queue deliveries and re-sends after an expired job fails without queue retries', async () => {
    const { LifecycleBucketShardRun } = await import(
      '@storage/events/lifecycle-rules/bucket-shard-run'
    )
    await activate()
    const name = 'history/queue-recovery.txt'
    const archived = randomUUID()
    const current = randomUUID()
    const archivedAt = new Date(Date.now() - 3 * DAY_MS)
    await seedHistory(name, archived, current, { archivedAt })
    const schema = `lifecycle_queue_${randomUUID().replaceAll('-', '')}`
    const options = {
      connectionString: getConfig().databaseURL,
      schema,
      supervise: false,
      schedule: false,
    }
    let boss = new PgBoss(options)
    const payload: LifecycleBucketShardRunPayload = {
      ...shard(bucketId),
      tenant: { ref: tenantId, host: '' },
    }
    const queue = LifecycleBucketShardRun.getQueueOptions()
    try {
      await boss.start()
      await boss.createQueue(queue.name, queue)
      const send = () =>
        boss.send(queue.name, payload, LifecycleBucketShardRun.getSendOptions(payload))
      const id = await send()
      expect(id).toBeTypeOf('string')
      await expect(send()).resolves.toBeNull()
      const [first] = await boss.fetch<LifecycleBucketShardRunPayload>(queue.name)
      expect(first.id).toBe(id)
      const input = await prepareAttempt(name, archived, archivedAt)
      expect(await helper.database.armLifecycleAttempt(input)).toBeDefined()
      // Duplicate delivery cannot take a live durable claim, even outside queue deduplication.
      await expect(runExecutor()).resolves.toMatchObject({ status: 'NOT_CLAIMED' })
      await expect(send()).resolves.toBeNull()
      await boss.stop()

      // Recreate the client with only persisted queue/state. Model elapsed lease
      // time without sleeping for the production job and claim expiry budgets.
      await helper.database.connection.query(
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
        boss.fetch<LifecycleBucketShardRunPayload>(queue.name, { includeMetadata: true })
      ).resolves.toEqual([])
      const resentId = await send()
      expect(resentId).toBeTypeOf('string')
      expect(resentId).not.toBe(id)
      const [recovered] = await boss.fetch<LifecycleBucketShardRunPayload>(queue.name)
      expect(recovered.id).toBe(resentId)
      const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')
      await expect(
        new NoncurrentLifecycleExecutor(helper.storage).run(recovered.data, {
          tenantLifecycleEnabled: true,
        })
      ).resolves.toMatchObject({ status: 'COMPLETED' })
      await boss.complete(queue.name, recovered.id)
      expect(deletes).toHaveBeenCalledOnce()
      await expect(
        helper.database.revalidateLifecycleRecoveryAttempt(
          input,
          input.continuation.batch!.inFlight!.attemptId,
          60_000
        )
      ).resolves.toBeUndefined()
      expect(await versions(name)).toEqual([{ version: current, current: true }])
      await expectBlobAbsent(name, archived)
      await expect(
        helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, current)
      ).resolves.toMatchObject({ size: 1 })
    } finally {
      await boss.stop()
      await helper.database.connection.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`)
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
    const body: Buffer[] = []
    for await (const chunk of object.body) body.push(Buffer.from(chunk))
    expect(Buffer.concat(body).toString()).toBe('replacement')
  })

  it.each([
    { recovery: false, bulk: false },
    { recovery: false, bulk: true },
    { recovery: true, bulk: false },
    { recovery: true, bulk: true },
  ])('fences promotion during lifecycle deletion (recovery=$recovery, bulk=$bulk)', async ({
    recovery,
    bulk,
  }) => {
    await activate()
    const name = 'history/promotion.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current)
    const deleteCurrent = () =>
      bulk
        ? helper.storage.from(bucketId).deleteObjects([{ path: name, versionId: current }])
        : helper.storage.from(bucketId).deleteObject(name, current)
    const deleteObjects = helper.adapter.deleteObjectsDetailed.bind(helper.adapter)
    const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')

    if (recovery) {
      deletes.mockImplementationOnce(async (_bucket, keys) =>
        keys.map((key) => ({ key, outcome: 'UNKNOWN' as const }))
      )
      await expect(runExecutor()).resolves.toMatchObject({ status: 'PARTIAL' })
      await makeDue()
    }
    deletes.mockImplementationOnce(async (bucket, keys, options) => {
      await expect(deleteCurrent()).rejects.toMatchObject({ code: 'ResourceLocked' })
      expect(await versions(name)).toContainEqual({ version: current, current: true })
      return deleteObjects(bucket, keys, options)
    })

    await expect(
      runExecutor({ now: () => new Date(Date.now() + 4 * DAY_MS) })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    await expectBlobAbsent(name, archived)
    await expect(
      helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, current)
    ).resolves.toMatchObject({ size: 1 })
    await deleteCurrent()
    expect(await versions(name)).toEqual([])
  })

  it('keeps a journal armed when a retry reports FAILED after an unknown delete result', async () => {
    await activate()
    const name = 'history/unknown-then-failed.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current)
    const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')
    deletes.mockImplementationOnce(async (_bucket, keys) =>
      keys.map((key) => ({ key, outcome: 'UNKNOWN' as const }))
    )
    deletes.mockImplementationOnce(async (_bucket, keys) =>
      keys.map((key) => ({ key, outcome: 'FAILED' as const, error: { message: 'denied' } }))
    )

    await expect(runExecutor()).resolves.toMatchObject({ status: 'PARTIAL' })
    await makeDue()
    await expect(
      runExecutor({ now: () => new Date(Date.now() + 4 * DAY_MS) })
    ).resolves.toMatchObject({ status: 'PARTIAL' })

    const [state] = await fixtures.readState(bucketId, 'continuation')
    expect(state.continuation.batch.inFlight).toBeDefined()
    expect(state.continuation.batch.versions[0].artifacts).toEqual([
      expect.objectContaining({ outcome: 'UNRESOLVED', error: 'denied' }),
      expect.objectContaining({ outcome: 'UNRESOLVED', error: 'denied' }),
    ])
    expect(await versions(name)).toContainEqual({ version: archived, current: false })
    await expect(helper.storage.from(bucketId).deleteObject(name, current)).rejects.toMatchObject({
      code: 'ResourceLocked',
    })
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

  it('pauses a due shard of a versioning-disabled bucket without failing', async () => {
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

    try {
      await expect(runExecutor()).resolves.toMatchObject({ status: 'PAUSED' })
      expect(deletes).not.toHaveBeenCalled()
    } finally {
      deletes.mockRestore()
    }

    const rows = await fixtures.readState(
      bucketId,
      'failure_count, last_error, next_run_at, continuation'
    )
    expect(rows).toEqual([
      { failure_count: 0, last_error: null, next_run_at: null, continuation: null },
    ])
    expect(await versions(name)).toEqual([{ version: current, current: true }])
  })

  it('completes with a warning when the backend rejects every deletion', async () => {
    await activate()
    const name = 'history/rejected.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current, { archivedAt: new Date(Date.now() - 3 * DAY_MS) })
    const deletes = vi
      .spyOn(helper.adapter, 'deleteObjectsDetailed')
      .mockImplementation(async (_bucket, keys) =>
        keys.map((key) => ({
          key,
          outcome: 'FAILED' as const,
          error: { code: 'AccessDenied', message: 'deletion denied' },
        }))
      )
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})

    try {
      await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
      expect(warning).toHaveBeenCalledOnce()
      expect(warning).toHaveBeenCalledWith(
        expect.anything(),
        '[Lifecycle] Backend rejected lifecycle deletions',
        expect.objectContaining({
          metadata: JSON.stringify({
            bucketId,
            failedArtifacts: 2,
            error: 'deletion denied',
          }),
        })
      )
    } finally {
      deletes.mockRestore()
      warning.mockRestore()
    }

    const [state] = await fixtures.readState(bucketId, 'failure_count, last_result')
    expect(state).toMatchObject({
      failure_count: 0,
      last_result: { counters: { objectVersionsDeleted: 0 } },
    })
    expect(await versions(name)).toEqual([
      { version: archived, current: false },
      { version: current, current: true },
    ])
    for (const version of [archived, current]) {
      await expect(
        helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, version)
      ).resolves.toMatchObject({ size: 1 })
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
      await seedHistory(name, archived, current, { archivedAt: new Date(Date.now() - 3 * DAY_MS) })
      const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')

      try {
        await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
        for (const [, keys] of deletes.mock.calls) {
          expect(keys.every((key) => Buffer.byteLength(key) <= 1024)).toBe(true)
        }
      } finally {
        deletes.mockRestore()
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
      [bucketId, name, version, new Date(Date.now() - 3 * DAY_MS)]
    )
    const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')

    try {
      await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
      expect(deletes).not.toHaveBeenCalled()
    } finally {
      deletes.mockRestore()
    }

    const [state] = await fixtures.readState(bucketId, 'continuation, last_result')
    expect(state.continuation).toBeNull()
    expect(state.last_result.counters).toMatchObject({
      deleteMarkersDeleted: 1,
      objectVersionsDeleted: 0,
    })
    expect(await versions(name)).toEqual([])
  })

  it('pauses without failing when the policy is removed by direct SQL between stage and arm', async () => {
    await activate()
    const name = 'history/config-removed.txt'
    const archived = randomUUID()
    await seedHistory(name, archived, randomUUID(), {
      archivedAt: new Date(Date.now() - 3 * DAY_MS),
    })
    const original = StoragePgDB.prototype.findLifecycleObjectVersions
    const staged = vi
      .spyOn(StoragePgDB.prototype, 'findLifecycleObjectVersions')
      .mockImplementationOnce(async function (this: StoragePgDB, ...args) {
        await fixtures.withServiceOperation('storage.s3.bucket.put_lifecycle', (transaction) =>
          transaction.query(
            'UPDATE storage.buckets SET lifecycle_configuration = NULL, lifecycle_configuration_generation = NULL WHERE id = $1',
            [bucketId]
          )
        )
        return original.apply(this, args)
      })
    const deletes = vi.spyOn(helper.adapter, 'deleteObjectsDetailed')

    try {
      await expect(runExecutor()).resolves.toMatchObject({ status: 'PAUSED' })
      expect(deletes).not.toHaveBeenCalled()
    } finally {
      staged.mockRestore()
      deletes.mockRestore()
    }
    const [state] = await fixtures.readState(bucketId, 'failure_count, last_error, claim_id')
    expect(state).toEqual({ failure_count: 0, last_error: null, claim_id: null })
  })

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
      await expect(
        helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, version)
      ).resolves.toMatchObject({ size: 1 })
    }
    const rows = await fixtures.readState(bucketId, 'continuation, claim_id, next_run_at')
    expect(rows).toEqual([{ continuation: null, claim_id: null, next_run_at: expect.any(Date) }])
  })

  it.each([true, false])('recovers an armed journal with artifacts absent=%s', async (absent) => {
    await activate()
    const name = 'history/recovery.txt'
    const archived = randomUUID()
    const current = randomUUID()
    const archivedAt = new Date(Date.now() - 3 * DAY_MS)
    await seedHistory(name, archived, current, { archivedAt })
    await uploadFixture(name, `${archived}.info`)
    const input = await prepareAttempt(name, archived, archivedAt)
    const { continuation: armed, ...identity } = input
    await expect(helper.database.armLifecycleAttempt(input)).resolves.toMatchObject({
      continuation: armed,
    })
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
      await expect(runExecutor()).resolves.toMatchObject({
        status: 'COMPLETED',
        runId: armed.runId,
      })
      expect(heads).not.toHaveBeenCalled()
      const dataKey = withOptionalVersion(`${tenantId}/${bucketId}/${name}`, archived)
      expect(deletes).toHaveBeenCalledExactlyOnceWith(
        storageS3Bucket,
        [dataKey, `${dataKey}.info`],
        { signal: expect.any(AbortSignal) }
      )
    } finally {
      heads.mockRestore()
      deletes.mockRestore()
    }

    const [state] = await fixtures.readState(bucketId, 'continuation, claim_id, last_result')
    expect(state).toMatchObject({
      continuation: null,
      claim_id: null,
      last_result: {
        runId: armed.runId,
        counters: { objectVersionsDeleted: 1, bytesDeleted: 1, batchesCompleted: 1 },
      },
    })
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    await expectBlobAbsent(name, archived)
    await expect(
      helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, current)
    ).resolves.toMatchObject({ size: 1 })
  })

  it('finishes an armed journal under a closed tenant gate and lifts the promotion fence', async () => {
    await activate()
    const name = 'history/gated-recovery.txt'
    const archived = randomUUID()
    const current = randomUUID()
    const archivedAt = new Date(Date.now() - 3 * DAY_MS)
    await seedHistory(name, archived, current, { archivedAt })
    const input = await prepareAttempt(name, archived, archivedAt)
    const { continuation: _armed, ...identity } = input
    await expect(helper.database.armLifecycleAttempt(input)).resolves.toBeDefined()
    await helper.database.releaseLifecycleShardClaim({
      ...identity,
      nextRunAt: '2000-01-01T00:00:00.000Z',
    })
    await expect(helper.storage.from(bucketId).deleteObject(name, current)).rejects.toMatchObject({
      code: 'ResourceLocked',
    })

    await expect(runExecutor({ tenantLifecycleEnabled: false })).resolves.toMatchObject({
      status: 'PAUSED',
    })

    expect(await versions(name)).toEqual([{ version: current, current: true }])
    await expectBlobAbsent(name, archived)
    const [state] = await fixtures.readState(bucketId, 'continuation, claim_id, next_run_at')
    expect(state.continuation?.batch).toBeUndefined()
    expect(state.next_run_at.getTime()).toBeGreaterThan(Date.now() + DAY_MS / 2)
    await helper.storage.from(bucketId).deleteObject(name, current)
    expect(await versions(name)).toEqual([])
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
  ] as const)('expires $label and preserves current bytes and sidecar-like siblings', async ({
    status,
    legacyNull,
    isVersioned,
    realClock,
  }) => {
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
    await expect(
      helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, current)
    ).resolves.toMatchObject({ size: 1 })
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
  })

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
    await expect(
      helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, null)
    ).resolves.toMatchObject({ size: 1 })

    await expect(runExecutor()).resolves.toMatchObject({ status: 'COMPLETED' })
    expect(await versions(name)).toEqual([marker])
    await expectBlobAbsent(name, null)
  })

  it.each([
    { label: 'null metadata', metadata: null, bytes: 0 },
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
    } finally {
      warning.mockRestore()
    }

    const [result] = await fixtures.readState(bucketId, 'continuation, last_result')
    expect(result).toMatchObject({
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

  it.each([1, 2])('re-reads unstored work after a failure before arm %i', async (failAt) => {
    await activate()
    const archivedAt = new Date(Date.now() - 3 * DAY_MS)
    const rows = ['retry/a', 'retry/b'].map((name) => ({
      name,
      archived: randomUUID(),
      current: randomUUID(),
    }))
    for (const row of rows) await seedHistory(row.name, row.archived, row.current, { archivedAt })
    const now = new Date()
    const failure = new Error('worker failed before arming')
    const arm = StoragePgDB.prototype.armLifecycleAttempt
    const attempts = new Set<string>()
    const arming = vi
      .spyOn(StoragePgDB.prototype, 'armLifecycleAttempt')
      .mockImplementation(async function (this: StoragePgDB, input) {
        // The public method delegates into a transaction with the same attempt ID.
        attempts.add(input.continuation.batch!.inFlight!.attemptId)
        if (attempts.size === failAt) throw failure
        return arm.call(this, input)
      })
    await expect(runExecutor({ pageSize: 1, now: () => now })).rejects.toBe(failure)
    arming.mockRestore()
    const [state] = await fixtures.readState(bucketId, 'continuation')
    if (failAt === 1) expect(state.continuation).toBeNull()
    else {
      expect(state.continuation).toMatchObject({
        snapshotAt: now.toISOString(),
        cursor: { name: rows[0].name },
        counters: { objectVersionsDeleted: 1 },
      })
      expect(state.continuation.batch).toBeUndefined()
    }
    await makeDue()
    const retryNow = new Date(now.getTime() + 15 * 60_000)
    await expect(runExecutor({ pageSize: 1, now: () => retryNow })).resolves.toMatchObject({
      status: 'COMPLETED',
    })
    const [completed] = await fixtures.readState(bucketId, 'last_result')
    expect(completed.last_result).toMatchObject({
      snapshotAt: (failAt === 1 ? retryNow : now).toISOString(),
      counters: { objectVersionsDeleted: 2 },
    })
    for (const row of rows) {
      expect(await versions(row.name)).toEqual([{ version: row.current, current: true }])
      await expectBlobAbsent(row.name, row.archived)
    }
  })

  it('caps an armed journal retry after lease expiry and recovers the preserved work', async () => {
    await activate()
    const name = 'history/expired-claim.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current, { archivedAt: new Date(Date.now() - 3 * DAY_MS) })
    await helper.database.connection.query(
      'UPDATE storage.bucket_lifecycle_states SET failure_count = 6 WHERE bucket_id = $1',
      [bucketId]
    )
    const now = new Date()
    const failure = new Error('backend request failed after lease expiry')
    const deletes = vi
      .spyOn(helper.adapter, 'deleteObjectsDetailed')
      .mockImplementationOnce(async () => {
        await helper.database.connection.query(
          "UPDATE storage.bucket_lifecycle_states SET claim_until = '2000-01-01' WHERE bucket_id = $1",
          [bucketId]
        )
        throw failure
      })

    await expect(runExecutor({ now: () => now })).rejects.toBe(failure)
    const [state] = await fixtures.readState(
      bucketId,
      'continuation, claim_id, failure_count, next_run_at'
    )
    expect(state).toMatchObject({
      claim_id: null,
      failure_count: 7,
      next_run_at: new Date(now.getTime() + 15 * 60_000),
      continuation: { batch: { inFlight: { attemptId: expect.any(String) } } },
    })
    expect(state.continuation.batch.versions).toEqual([
      expect.objectContaining({ name, version: archived }),
    ])
    expect(await versions(name)).toHaveLength(2)

    deletes.mockRestore()
    await makeDue()
    await expect(
      runExecutor({ now: () => new Date(now.getTime() + 15 * 60_000) })
    ).resolves.toMatchObject({ status: 'COMPLETED' })
    await expect(fixtures.readState(bucketId, 'continuation, failure_count')).resolves.toEqual([
      { continuation: null, failure_count: 0 },
    ])
    expect(await versions(name)).toEqual([{ version: current, current: true }])
    await expectBlobAbsent(name, archived)
    await expect(
      helper.adapter.headObject(storageS3Bucket, `${tenantId}/${bucketId}/${name}`, current)
    ).resolves.toMatchObject({ size: 1 })
  })

  it('persists incomplete-deletion backoff and clears the failure after recovery', async () => {
    await activate()
    const name = 'history/retry.txt'
    const archived = randomUUID()
    const current = randomUUID()
    await seedHistory(name, archived, current, { archivedAt: new Date(Date.now() - 3 * DAY_MS) })
    await uploadFixture(name, `${archived}.info`)
    const dataKey = withOptionalVersion(`${tenantId}/${bucketId}/${name}`, archived)
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

    const run = () => runExecutor({ now: () => now })
    try {
      for (const [index, delayMinutes] of [5, 10].entries()) {
        await expect(run()).resolves.toMatchObject({ status: 'PARTIAL' })
        const nextRunAt = new Date(now.getTime() + delayMinutes * 60_000)
        const [state] = await fixtures.readState(
          bucketId,
          'continuation, claim_id, failure_count, last_error, next_run_at'
        )
        expect(state).toMatchObject({
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
        expect(state.continuation.batch.versions[0].artifacts).toEqual([
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

        await makeDue()
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

    const [result] = await fixtures.readState(
      bucketId,
      'continuation, failure_count, last_error, last_result'
    )
    expect(result).toMatchObject({
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
