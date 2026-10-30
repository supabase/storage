import { randomUUID } from 'node:crypto'
import type { BucketLifecycleConfiguration } from '@storage/schemas'
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

  async function writePolicy(writer: 'API' | 'SQL', policy: BucketLifecycleConfiguration | null) {
    if (writer === 'API') {
      return policy === null
        ? helper.database.deleteLifecycleConfiguration(bucketId)
        : helper.database.putLifecycleConfiguration(bucketId, policy)
    }
    await fixtures.withServiceOperation('storage.s3.bucket.put_lifecycle', (transaction) =>
      transaction.query(
        `UPDATE storage.buckets
       SET lifecycle_configuration = $2::jsonb, lifecycle_configuration_generation = $3::uuid
       WHERE id = $1`,
        [
          bucketId,
          policy === null ? null : JSON.stringify(policy),
          policy === null ? null : randomUUID(),
        ]
      )
    )
    return helper.database.findLifecycleBucket(bucketId)
  }

  it.each([
    { status: 'Enabled', due: true },
    { status: 'Disabled', due: null },
    { status: null, due: null },
  ] as const)(
    'handles a direct SQL bucket INSERT with policy status $status',
    async ({ status, due }) => {
      await withDeleteEnabled(helper.database.connection, (transaction) =>
        transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
      )
      const policy =
        status === null
          ? null
          : {
              rules: [{ ...configuration.rules[0], status }],
            }
      await fixtures.withServiceOperation('storage.s3.bucket.put_lifecycle', (transaction) =>
        transaction.query(
          `INSERT INTO storage.buckets (id, name, versioning_status,
           lifecycle_configuration, lifecycle_configuration_generation)
         VALUES ($1, $1, 'ENABLED', $2::jsonb, $3::uuid)`,
          [
            bucketId,
            policy === null ? null : JSON.stringify(policy),
            policy === null ? null : randomUUID(),
          ]
        )
      )

      await expect(
        fixtures.readState(bucketId, 'next_run_at <= clock_timestamp() AS due')
      ).resolves.toEqual(policy === null ? [] : [{ due }])
    }
  )

  it.each(['API', 'SQL'] as const)(
    'keeps state dormant when configured on a versioning-disabled bucket via %s',
    async (writer) => {
      await writePolicy(writer, configuration)
      await expect(fixtures.readState(bucketId, 'next_run_at')).resolves.toEqual([
        { next_run_at: null },
      ])
      await expect(
        helper.database.findLifecycleDispatchState({ runBudgetMs: 20_000 })
      ).resolves.toMatchObject({
        nextDispatchAt: null,
      })
    }
  )

  it('wakes an existing policy when versioning is enabled without another policy PUT', async () => {
    const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    await helper.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })

    await expect(
      fixtures.readState(bucketId, 'next_run_at <= clock_timestamp() AS due')
    ).resolves.toEqual([{ due: true }])
    await expect(helper.database.listDueLifecycleBuckets(100)).resolves.toContainEqual(bucketId)
    const retried = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    expect(retried.lifecycle_configuration_generation).toBe(
      configured.lifecycle_configuration_generation
    )
    await expect(
      fixtures.readState(bucketId, 'next_run_at <= clock_timestamp() AS due')
    ).resolves.toEqual([{ due: true }])
  })

  it.each(['DISABLED', 'ENABLED'] as const)(
    'deletes lifecycle state with an empty %s bucket',
    async (status) => {
      await fixtures.setStatus(bucketId, status)
      await helper.database.putLifecycleConfiguration(bucketId, configuration)

      await expect(helper.storage.deleteBucket(bucketId)).resolves.toBe(1)
      await expect(fixtures.readState(bucketId, '1')).resolves.toHaveLength(0)
      await expect(
        helper.database.connection.query('SELECT 1 FROM storage.buckets WHERE id = $1', [bucketId])
      ).resolves.toMatchObject({ rowCount: 0 })
    }
  )

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

  it.each(['API', 'SQL'] as const)(
    'retains and wakes an armed attempt when the lifecycle configuration is deleted via %s',
    async (writer) => {
      const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
      const continuation = armContinuation(configured.lifecycle_configuration_generation!)
      await helper.database.connection.query(
        `UPDATE storage.bucket_lifecycle_states
       SET continuation = $2::jsonb, next_run_at = '2099-01-01T00:00:00Z'
       WHERE bucket_id = $1`,
        [bucketId, JSON.stringify(continuation)]
      )

      const deleted = await writePolicy(writer, null)
      expect(deleted).toMatchObject({ lifecycle_configuration: null })

      const [state] = await fixtures.readState(
        bucketId,
        'continuation, next_run_at <= clock_timestamp() AS woken'
      )
      expect(state).toEqual({ continuation, woken: true })

      const retry = await helper.database.deleteLifecycleConfiguration(bucketId)
      expect(retry.lifecycle_configuration).toBeNull()
      expect(await fixtures.readState(bucketId, '1')).toHaveLength(1)
    }
  )

  it.each(['API', 'SQL'] as const)(
    'retains an armed attempt and wakes it when the policy is replaced via %s',
    async (writer) => {
      const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
      const continuation = armContinuation(configured.lifecycle_configuration_generation!)
      await helper.database.connection.query(
        `UPDATE storage.bucket_lifecycle_states
       SET continuation = $2::jsonb, claim_id = gen_random_uuid(), claim_until = '2099-01-01',
           failure_count = 3, last_error = '"boom"', next_run_at = '2099-01-01'
       WHERE bucket_id = $1`,
        [bucketId, JSON.stringify(continuation)]
      )

      await writePolicy(writer, {
        rules: [{ ...configuration.rules[0], status: 'Disabled' }],
      })

      await expect(
        fixtures.readState(
          bucketId,
          `continuation, claim_id IS NOT NULL AS claimed, claim_until = '2099-01-01' AS leased,
         failure_count, last_error, next_run_at <= clock_timestamp() AS woken`
        )
      ).resolves.toEqual([
        {
          continuation,
          claimed: true,
          leased: true,
          failure_count: 3,
          last_error: 'boom',
          woken: true,
        },
      ])
    }
  )

  it.each(
    (['API', 'SQL'] as const).flatMap((writer) =>
      [
        {
          format: 'version 2',
          continuation: { continuationVersion: 2, recovery: { pending: true } },
        },
        { format: 'missing version', continuation: {} },
      ].flatMap(({ format, continuation }) =>
        (['delete', 'disable'] as const).map((action) => ({ writer, format, continuation, action }))
      )
    )
  )(
    'preserves $format continuation on $action via $writer',
    async ({ writer, continuation, action }) => {
      await fixtures.setStatus(bucketId, 'ENABLED')
      await helper.database.putLifecycleConfiguration(bucketId, configuration)
      const claimId = randomUUID()
      await helper.database.connection.query(
        `UPDATE storage.bucket_lifecycle_states
       SET continuation = $2::jsonb, claim_id = $3::uuid, claim_until = '2099-01-01',
           failure_count = 3, last_error = '"boom"', next_run_at = '2099-01-01'
       WHERE bucket_id = $1`,
        [bucketId, JSON.stringify(continuation), claimId]
      )

      const policy =
        action === 'delete'
          ? null
          : { rules: [{ ...configuration.rules[0], status: 'Disabled' as const }] }
      await writePolicy(writer, policy)
      await expect(
        fixtures.readState(
          bucketId,
          `continuation, claim_id, claim_until = '2099-01-01' AS leased,
         failure_count, last_error, next_run_at <= clock_timestamp() AS woken`
        )
      ).resolves.toEqual([
        {
          continuation,
          claim_id: claimId,
          leased: true,
          failure_count: 3,
          last_error: 'boom',
          woken: true,
        },
      ])
    }
  )

  it.each(['API', 'SQL'] as const)(
    'clears an idle claimed run and wakes it when the policy is replaced via %s',
    async (writer) => {
      await fixtures.setStatus(bucketId, 'ENABLED')
      const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
      await helper.database.connection.query(
        `UPDATE storage.bucket_lifecycle_states
       SET continuation = $2::jsonb, claim_id = gen_random_uuid(), claim_until = '2099-01-01',
           failure_count = 3, last_error = '"boom"', next_run_at = '2099-01-01'
       WHERE bucket_id = $1`,
        [bucketId, JSON.stringify(newContinuation(configured.lifecycle_configuration_generation!))]
      )

      await writePolicy(writer, {
        rules: [
          {
            ...configuration.rules[0],
            noncurrentVersionExpiration: { noncurrentDays: 7 },
          },
        ],
      })

      await expect(
        fixtures.readState(
          bucketId,
          `continuation, claim_id, claim_until, failure_count, last_error,
         next_run_at <= clock_timestamp() AS woken`
        )
      ).resolves.toEqual([
        {
          continuation: null,
          claim_id: null,
          claim_until: null,
          failure_count: 0,
          last_error: null,
          woken: true,
        },
      ])
    }
  )

  it.each(['API', 'SQL'] as const)(
    'removes idle state when a policy is deleted via %s',
    async (writer) => {
      await helper.database.putLifecycleConfiguration(bucketId, configuration)
      await writePolicy(writer, null)
      await expect(fixtures.readState(bucketId, '1')).resolves.toEqual([])
    }
  )

  it('repairs missing state on an unchanged API PUT without rotating the generation', async () => {
    const bucket = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    await helper.database.connection.query(
      'DELETE FROM storage.bucket_lifecycle_states WHERE bucket_id = $1',
      [bucketId]
    )
    const retried = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    expect(retried.lifecycle_configuration_generation).toBe(
      bucket.lifecycle_configuration_generation
    )
    await expect(fixtures.readState(bucketId, 'next_run_at')).resolves.toEqual([
      { next_run_at: null },
    ])
  })

  it('preserves healthy state on a SQL update that does not change the generation', async () => {
    await fixtures.setStatus(bucketId, 'ENABLED')
    await helper.database.putLifecycleConfiguration(bucketId, configuration)
    await helper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states SET next_run_at = '2099-01-01',
       claim_id = gen_random_uuid(), claim_until = '2099-01-01', failure_count = 3,
       last_error = '{"message":"preserved"}' WHERE bucket_id = $1`,
      [bucketId]
    )
    const before = await fixtures.readState(bucketId, '*')
    await fixtures.withServiceOperation('storage.s3.bucket.put_lifecycle', (transaction) =>
      transaction.query(
        'UPDATE storage.buckets SET lifecycle_configuration_generation = lifecycle_configuration_generation WHERE id = $1',
        [bucketId]
      )
    )
    expect(await fixtures.readState(bucketId, '*')).toEqual(before)
  })

  it('resets run state when the state row has an older generation', async () => {
    await fixtures.setStatus(bucketId, 'ENABLED')
    const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
    await helper.database.connection.query(
      `UPDATE storage.bucket_lifecycle_states SET generation = gen_random_uuid(),
       next_run_at = '2099-01-01', claim_id = gen_random_uuid(), claim_until = '2099-01-01',
       failure_count = 3, last_error = '{"message":"stale"}' WHERE bucket_id = $1`,
      [bucketId]
    )

    await helper.database.connection.query('SELECT storage.reconcile_lifecycle_state($1, false)', [
      bucketId,
    ])
    await expect(
      fixtures.readState(
        bucketId,
        `generation, claim_id, claim_until, failure_count, last_error,
         next_run_at <= clock_timestamp() AS woken`
      )
    ).resolves.toEqual([
      {
        generation: configured.lifecycle_configuration_generation,
        claim_id: null,
        claim_until: null,
        failure_count: 0,
        last_error: null,
        woken: true,
      },
    ])
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

    it.each(['SUSPENDED', 'ENABLED'] as const)(
      'wakes a dormant state when a %s bucket is enabled',
      async (status) => {
        await fixtures.setStatus(bucketId, status)
        await helper.database.putLifecycleConfiguration(bucketId, configuration)
        await setState('next_run_at = NULL')

        await enable()
        await expect(
          fixtures.readState(bucketId, 'next_run_at <= clock_timestamp() AS due')
        ).resolves.toEqual([{ due: true }])
      }
    )

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

    it('keeps idle state when a later trigger skips the delete', async () => {
      await helper.database.putLifecycleConfiguration(bucketId, configuration)
      const name = `skip_bucket_delete_${randomUUID().replaceAll('-', '')}`
      const { connection } = helper.database
      await connection.query(
        `CREATE FUNCTION public.${name}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF OLD.id = TG_ARGV[0] THEN RETURN NULL; END IF;
           RETURN OLD;
         END;
         $$`
      )
      try {
        const { rows } = await connection.query<{ literal: string }>(
          'SELECT quote_literal($1::text) AS literal',
          [bucketId]
        )
        await connection.query(
          `CREATE TRIGGER ${name} BEFORE DELETE ON storage.buckets
           FOR EACH ROW EXECUTE FUNCTION public.${name}(${rows[0].literal})`
        )

        await expect(deleteBucketRow()).resolves.toMatchObject({ rowCount: 0 })
        await expect(fixtures.readState(bucketId, '1')).resolves.toHaveLength(1)
        await expect(
          connection.query('SELECT 1 FROM storage.buckets WHERE id = $1', [bucketId])
        ).resolves.toMatchObject({ rowCount: 1 })
      } finally {
        await connection.query(`DROP TRIGGER IF EXISTS ${name} ON storage.buckets`)
        await connection.query(`DROP FUNCTION IF EXISTS public.${name}()`)
      }
    })

    it('blocks deletion and preserves an armed attempt', async () => {
      const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
      const continuation = armContinuation(configured.lifecycle_configuration_generation!)
      await helper.database.connection.query(
        `UPDATE storage.bucket_lifecycle_states
         SET continuation = $2::jsonb, next_run_at = clock_timestamp() WHERE bucket_id = $1`,
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

    it.each([{ continuationVersion: 999 }, { continuationVersion: 2, batch: {} }, {}])(
      'preserves unreadable continuation %j',
      async (continuation) => {
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
          helper.database.connection.query('SELECT 1 FROM storage.buckets WHERE id = $1', [
            bucketId,
          ])
        ).resolves.toMatchObject({ rowCount: 1 })
      }
    )

    it.each([
      { name: 'cursor-only', staged: false },
      { name: 'staged unarmed batch', staged: true },
    ])('removes version 1 state with a $name continuation', async ({ staged }) => {
      const configured = await helper.database.putLifecycleConfiguration(bucketId, configuration)
      const generation = configured.lifecycle_configuration_generation!
      const armed = armContinuation(generation)
      const continuation = staged
        ? (({ inFlight: _inFlight, ...batch }) => ({ ...armed, batch }))(armed.batch!)
        : newContinuation(generation)
      await helper.database.connection.query(
        'UPDATE storage.bucket_lifecycle_states SET continuation = $2::jsonb WHERE bucket_id = $1',
        [bucketId, JSON.stringify(continuation)]
      )

      await expect(deleteBucketRow()).resolves.toMatchObject({ rowCount: 1 })
      await expect(fixtures.readState(bucketId, '1')).resolves.toHaveLength(0)
    })
  })
})
