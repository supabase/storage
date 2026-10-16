import { randomUUID } from 'node:crypto'
import { armedContinuation, newContinuation } from './utils/lifecycle-continuation'
import { useLifecycleStorage } from './utils/lifecycle-versioning'
import { withDeleteEnabled } from './utils/storage'

describe('bucket lifecycle controls', () => {
  const fixtures = useLifecycleStorage('bucket-lifecycle')
  const helper = fixtures.helper
  let bucketId: string

  beforeEach(async () => {
    bucketId = fixtures.bucketId
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

  it('keeps state dormant when configured on a versioning-disabled bucket', async () => {
    await helper.database.putLifecycleConfiguration(bucketId, configuration)
    await expect(fixtures.readState(bucketId, 'next_run_at')).resolves.toEqual([
      { next_run_at: null },
    ])
    await expect(helper.database.findLifecycleDispatchState()).resolves.toMatchObject({
      nextDispatchAt: null,
    })
  })

  it('wakes an existing policy when versioning is enabled without another policy PUT', async () => {
    const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    await helper.storage.updateBucket(bucketId, { versioning_status: 'ENABLED' })

    await expect(
      fixtures.readState(bucketId, 'next_run_at <= clock_timestamp() AS due')
    ).resolves.toEqual([{ due: true }])
    await expect(helper.database.listDueLifecycleShards(100)).resolves.toContainEqual({
      bucketId,
      scanKind: 'NONCURRENT',
      shardEpoch: '1',
      shardId: 0,
    })
    const retried = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    expect(retried.lifecycle_configuration_generation).toBe(
      configured.lifecycle_configuration_generation
    )
    await expect(
      fixtures.readState(bucketId, 'next_run_at <= clock_timestamp() AS due')
    ).resolves.toEqual([{ due: true }])
  })

  it.each([
    'DISABLED',
    'ENABLED',
  ] as const)('deletes lifecycle state with an empty %s bucket', async (status) => {
    await fixtures.setStatus(bucketId, status)
    await helper.database.putLifecycleConfiguration(bucketId, configuration)

    await expect(helper.storage.deleteBucket(bucketId)).resolves.toBe(1)
    await expect(fixtures.readState(bucketId, '1')).resolves.toHaveLength(0)
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
    await expect(fixtures.readState(bucketId, '1')).resolves.toHaveLength(1)
    await expect(
      helper.database.connection.query('SELECT 1 FROM storage.buckets WHERE id = $1', [bucketId])
    ).resolves.toMatchObject({ rowCount: 1 })
  })

  function armContinuation(generation: string) {
    const now = new Date().toISOString()
    return armedContinuation(
      generation,
      [{ id: randomUUID(), name: 'object', version: randomUUID(), isDeleteMarker: false }],
      { name: 'object', archivedAt: now },
      { snapshotAt: now, startedAt: now }
    )
  }

  it('blocks bucket deletion while an armed attempt is in flight', async () => {
    const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    const continuation = armContinuation(configured.lifecycle_configuration_generation!)
    await helper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states
       SET continuation = $2::jsonb, next_run_at = clock_timestamp()
       WHERE bucket_id = $1`,
      [bucketId, JSON.stringify(continuation)]
    )

    await expect(helper.storage.deleteBucket(bucketId)).rejects.toMatchObject({
      code: 'ResourceReferenced',
    })
    await expect(fixtures.readState(bucketId, 'continuation')).resolves.toEqual([{ continuation }])
  })

  it('retains and wakes an armed attempt when the lifecycle configuration is deleted', async () => {
    const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    const continuation = armContinuation(configured.lifecycle_configuration_generation!)
    await helper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states
       SET continuation = $2::jsonb, next_run_at = '2099-01-01T00:00:00Z'
       WHERE bucket_id = $1`,
      [bucketId, JSON.stringify(continuation)]
    )

    const deleted = await helper.database.deleteLifecycleConfiguration(bucketId)
    expect(deleted).toMatchObject({ lifecycle_configuration: null })

    const [state] = await fixtures.readState(
      bucketId,
      'continuation, next_run_at <= clock_timestamp() AS woken'
    )
    expect(state).toEqual({ continuation, woken: true })

    const retry = await helper.database.deleteLifecycleConfiguration(bucketId)
    expect(retry.lifecycle_configuration).toBeNull()
    expect(await fixtures.readState(bucketId, '1')).toHaveLength(1)
  })

  describe('wake trigger on versioning enable', () => {
    const state = () => fixtures.readState(bucketId, 'next_run_at, continuation')
    const enable = () => helper.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
    const setState = (sets: string, values: unknown[] = []) =>
      helper.database.connection.query(
        `UPDATE storage.bucket_lifecycle_states SET ${sets} WHERE bucket_id = $1`,
        [bucketId, ...values]
      )

    it('leaves a healthy future schedule untouched', async () => {
      await helper.database.putLifecycleConfiguration(bucketId, configuration)
      await setState("next_run_at = '2099-01-01T00:00:00Z'")
      const before = await state()

      await enable()
      expect(await state()).toEqual(before)
    })

    it('leaves a row with a journal in flight untouched', async () => {
      const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
      const continuation = armContinuation(configured.lifecycle_configuration_generation!)
      await setState("continuation = $2::jsonb, next_run_at = '2099-01-01T00:00:00Z'", [
        JSON.stringify(continuation),
      ])
      const before = await state()

      await enable()
      expect(await state()).toEqual(before)
    })

    it('does not fire when a suspended bucket is re-enabled', async () => {
      await helper.database.putLifecycleConfiguration(bucketId, configuration)
      await fixtures.setStatus(bucketId, 'SUSPENDED')
      await setState('next_run_at = NULL')

      await enable()
      await expect(state()).resolves.toMatchObject([{ next_run_at: null }])
    })

    it('keeps a policy with only disabled rules dormant', async () => {
      await helper.database.putLifecycleConfiguration(bucketId, {
        rules: [{ ...configuration.rules[0], status: 'Disabled' as const }],
      })

      await enable()
      await expect(state()).resolves.toMatchObject([{ next_run_at: null }])
    })
  })

  describe('direct bucket deletion by builds without lifecycle awareness', () => {
    const deleteBucketRow = () =>
      withDeleteEnabled(helper.database.connection, (transaction) =>
        transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
      )

    it('removes an idle state row with the bucket', async () => {
      await helper.database.putLifecycleConfiguration(bucketId, configuration)

      await expect(deleteBucketRow()).resolves.toMatchObject({ rowCount: 1 })
      await expect(fixtures.readState(bucketId, '1')).resolves.toHaveLength(0)
    })

    it('keeps other buckets idle state rows', async () => {
      const otherId = await fixtures.createBucket()
      await helper.database.putLifecycleConfiguration(bucketId, configuration)
      await helper.database.putLifecycleConfiguration(otherId, configuration)

      await expect(deleteBucketRow()).resolves.toMatchObject({ rowCount: 1 })
      await expect(fixtures.readState(bucketId, '1')).resolves.toHaveLength(0)
      await expect(fixtures.readState(otherId, '1')).resolves.toHaveLength(1)
    })

    it('still refuses while a journal is in flight', async () => {
      const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
      const continuation = armContinuation(configured.lifecycle_configuration_generation!)
      await helper.database.connection.query(
        'UPDATE storage.bucket_lifecycle_states SET continuation = $2::jsonb, next_run_at = clock_timestamp() WHERE bucket_id = $1',
        [bucketId, JSON.stringify(continuation)]
      )

      await expect(deleteBucketRow()).rejects.toMatchObject({ code: '23503' })
      await expect(fixtures.readState(bucketId, '1')).resolves.toHaveLength(1)
    })

    it.each([
      { continuationVersion: 999 },
      { continuationVersion: 1, batch: {} },
      {},
    ])('preserves unreadable continuation %j', async (continuation) => {
      await helper.database.putLifecycleConfiguration(bucketId, configuration)
      await helper.database.connection.query(
        'UPDATE storage.bucket_lifecycle_states SET continuation = $2::jsonb WHERE bucket_id = $1',
        [bucketId, JSON.stringify(continuation)]
      )

      await expect(deleteBucketRow()).rejects.toMatchObject({ code: '23503' })
      await expect(fixtures.readState(bucketId, 'continuation')).resolves.toEqual([
        { continuation },
      ])
      await expect(
        helper.database.connection.query('SELECT 1 FROM storage.buckets WHERE id = $1', [bucketId])
      ).resolves.toMatchObject({ rowCount: 1 })
    })

    it('leaves structured idle state for a lifecycle-aware writer to validate and delete', async () => {
      const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
      const continuation = newContinuation(configured.lifecycle_configuration_generation!)
      await helper.database.connection.query(
        'UPDATE storage.bucket_lifecycle_states SET continuation = $2::jsonb WHERE bucket_id = $1',
        [bucketId, JSON.stringify(continuation)]
      )

      await expect(deleteBucketRow()).rejects.toMatchObject({ code: '23503' })
      await expect(fixtures.readState(bucketId, 'continuation')).resolves.toEqual([
        { continuation },
      ])
      await expect(helper.storage.deleteBucket(bucketId)).resolves.toBe(1)
      await expect(fixtures.readState(bucketId, '1')).resolves.toHaveLength(0)
    })
  })
})
