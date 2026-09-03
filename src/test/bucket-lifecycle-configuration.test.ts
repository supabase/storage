import { randomUUID } from 'node:crypto'
import { ROUTE_OPERATIONS } from '../http/routes/operations'
import { StoragePgDB } from '../storage/database'
import { useStorage, withDeleteEnabled } from './utils/storage'

describe('bucket lifecycle configuration persistence', () => {
  let currentOperation = ''
  const helper = useStorage({ operation: () => currentOperation })
  let bucketId: string

  async function withOperation<T>(operation: string, fn: () => Promise<T>): Promise<T> {
    const previousOperation = currentOperation
    currentOperation = operation
    try {
      return await fn()
    } finally {
      currentOperation = previousOperation
    }
  }

  function putLifecycleConfiguration(
    configuration: Parameters<typeof helper.database.putLifecycleConfiguration>[1]
  ) {
    return withOperation(ROUTE_OPERATIONS.S3_PUT_BUCKET_LIFECYCLE, () =>
      helper.database.putLifecycleConfiguration(bucketId, configuration)
    )
  }

  function deleteLifecycleConfiguration() {
    return withOperation(ROUTE_OPERATIONS.S3_DELETE_BUCKET_LIFECYCLE, () =>
      helper.database.deleteLifecycleConfiguration(bucketId)
    )
  }

  beforeEach(async () => {
    bucketId = `bucket-lifecycle-configuration-${randomUUID()}`
    await helper.database.createBucket({ id: bucketId, name: bucketId })
  })

  afterEach(async () => {
    await withDeleteEnabled(helper.database.connection, async (transaction) => {
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
    })
  })

  const rules = [
    {
      id: 'expire-history',
      status: 'Enabled' as const,
      filter: {},
      noncurrentVersionExpiration: {
        noncurrentDays: 30,
        newerNoncurrentVersions: 2,
      },
    },
    {
      id: 'keep-short-history',
      status: 'Disabled' as const,
      filter: {},
      noncurrentVersionExpiration: { noncurrentDays: 7 },
    },
  ]

  it('stores canonical policy state and preserves generation for an equivalent PUT', async () => {
    const first = await putLifecycleConfiguration({ rules })

    expect(first.changed).toBe(true)
    expect(first.bucket.lifecycle_configuration).toEqual({ rules })
    expect(first.bucket.lifecycle_configuration_generation).toMatch(/^[0-9a-f]{8}-[0-9a-f-]{27}$/i)
    const beforeRetry = await helper.database.findBucketById(bucketId, 'updated_at')

    const retry = await putLifecycleConfiguration({
      rules: [...rules].reverse(),
    })
    expect(retry.changed).toBe(false)
    expect(retry.bucket.lifecycle_configuration_generation).toBe(
      first.bucket.lifecycle_configuration_generation
    )
    expect(retry.bucket.lifecycle_configuration?.rules).toEqual(rules)
    await expect(helper.database.findBucketById(bucketId, 'updated_at')).resolves.toEqual(
      beforeRetry
    )

    await expect(helper.database.findLifecycleBucket(bucketId)).resolves.toEqual(retry.bucket)
  })

  it('rotates generation only when canonical policy content changes', async () => {
    const first = await putLifecycleConfiguration({ rules })
    const replacement = await putLifecycleConfiguration({
      rules: [
        {
          ...rules[0],
          noncurrentVersionExpiration: { noncurrentDays: 60 },
        },
      ],
    })

    expect(replacement.changed).toBe(true)
    expect(replacement.bucket.lifecycle_configuration_generation).not.toBe(
      first.bucket.lifecycle_configuration_generation
    )
  })

  it('repairs a stored rule that is missing its expiration', async () => {
    const malformedGeneration = randomUUID()
    const transaction = await helper.database.connection.transaction()

    try {
      await withOperation(ROUTE_OPERATIONS.S3_PUT_BUCKET_LIFECYCLE, async () => {
        await helper.database.connection.setScope(transaction)
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
                  id: rules[0].id,
                  status: rules[0].status,
                  filter: rules[0].filter,
                },
              ],
            }),
            malformedGeneration,
          ]
        )
      })
      await transaction.commit()
    } catch (error) {
      await transaction.rollback()
      throw error
    }

    const repaired = await putLifecycleConfiguration({ rules: [rules[0]] })

    expect(repaired).toMatchObject({
      changed: true,
      bucket: {
        lifecycle_configuration: { rules: [rules[0]] },
      },
    })
    expect(repaired.bucket.lifecycle_configuration_generation).not.toBe(malformedGeneration)
  })

  it('clears the policy pair and makes repeated deletion a no-op', async () => {
    await putLifecycleConfiguration({ rules })

    const deleted = await deleteLifecycleConfiguration()
    expect(deleted).toMatchObject({
      changed: true,
      bucket: {
        lifecycle_configuration: null,
        lifecycle_configuration_generation: null,
      },
    })

    await expect(deleteLifecycleConfiguration()).resolves.toMatchObject({
      changed: false,
      bucket: {
        lifecycle_configuration: null,
        lifecycle_configuration_generation: null,
      },
    })
  })

  it('sets the protected operation for direct lifecycle writes', async () => {
    const stored = await helper.database.putLifecycleConfiguration(bucketId, { rules })

    expect(stored.changed).toBe(true)
    await expect(helper.database.deleteLifecycleConfiguration(bucketId)).resolves.toMatchObject({
      changed: true,
      bucket: {
        lifecycle_configuration: null,
        lifecycle_configuration_generation: null,
      },
    })
  })

  it('restores the operation after a lifecycle write in a caller-owned service-role transaction', async () => {
    const transaction = await helper.database.connection.transaction()
    const previousOperation = 'storage.object.get'

    try {
      await helper.database.connection.setScope(transaction)
      await transaction.query(`SELECT set_config('storage.operation', $1, true)`, [
        previousOperation,
      ])
      const transactionDatabase = new StoragePgDB(helper.database.connection, {
        tenantId: helper.database.tenantId,
        host: helper.database.tenantHost,
        latestMigration: 'validate-bucket-lifecycle-constraints',
        tnx: transaction,
      })

      await transactionDatabase.putLifecycleConfiguration(bucketId, { rules })

      await expect(
        transaction.query<{ operation: string }>(
          `SELECT current_setting('storage.operation', true) AS operation`
        )
      ).resolves.toMatchObject({ rows: [{ operation: previousOperation }] })
      await expect(
        transaction.query(
          `UPDATE storage.buckets
           SET lifecycle_configuration = $2::jsonb,
               lifecycle_configuration_generation = $3::uuid
           WHERE id = $1`,
          [
            bucketId,
            JSON.stringify({
              rules: [
                {
                  ...rules[0],
                  noncurrentVersionExpiration: { noncurrentDays: 31 },
                },
              ],
            }),
            randomUUID(),
          ]
        )
      ).rejects.toMatchObject({ code: '42501' })
    } finally {
      await transaction.rollback()
    }
  })

  it.each([
    {
      label: 'PUT',
      operation: ROUTE_OPERATIONS.S3_PUT_BUCKET_LIFECYCLE,
      prepare: async () => {},
      mutate: (database: StoragePgDB) => database.putLifecycleConfiguration(bucketId, { rules }),
    },
    {
      label: 'DELETE',
      operation: ROUTE_OPERATIONS.S3_DELETE_BUCKET_LIFECYCLE,
      prepare: () => putLifecycleConfiguration({ rules }),
      mutate: (database: StoragePgDB) => database.deleteLifecycleConfiguration(bucketId),
    },
  ])('keeps the exact S3 operation active during a lifecycle $label write', async (testCase) => {
    await testCase.prepare()
    const transaction = await helper.database.connection.transaction()
    const originalQuery = transaction.query.bind(transaction)
    let operationAtWrite: string | undefined
    const querySpy = vi
      .spyOn(transaction, 'query')
      .mockImplementation(async (statement, options) => {
        const text = typeof statement === 'string' ? statement : statement.text
        if (
          text.includes('UPDATE storage.buckets') &&
          text.includes('SET lifecycle_configuration')
        ) {
          const observed = await originalQuery<{ operation: string }>(
            `SELECT current_setting('storage.operation', true) AS operation`
          )
          operationAtWrite = observed.rows[0]?.operation
        }
        return originalQuery(statement, options)
      })

    try {
      await withOperation(testCase.operation, async () => {
        await helper.database.connection.setScope(transaction)
        const transactionDatabase = new StoragePgDB(helper.database.connection, {
          tenantId: helper.database.tenantId,
          host: helper.database.tenantHost,
          latestMigration: 'validate-bucket-lifecycle-constraints',
          tnx: transaction,
        })
        await testCase.mutate(transactionDatabase)
      })

      expect(operationAtWrite).toBe(testCase.operation)
    } finally {
      querySpy.mockRestore()
      await transaction.rollback()
    }
  })

  it('keeps the configuration pair null by default and enforces its database shape', async () => {
    const stored = await helper.database.connection.query<{
      lifecycle_configuration: unknown | null
      lifecycle_configuration_generation: string | null
    }>(
      `SELECT lifecycle_configuration, lifecycle_configuration_generation
       FROM storage.buckets
       WHERE id = $1`,
      [bucketId]
    )
    expect(stored.rows[0]).toEqual({
      lifecycle_configuration: null,
      lifecycle_configuration_generation: null,
    })

    const constraints = await helper.database.connection.query<{
      conname: string
      convalidated: boolean
    }>(
      `SELECT conname, convalidated
       FROM pg_catalog.pg_constraint
       WHERE conrelid = 'storage.buckets'::regclass
         AND conname = ANY($1::text[])
       ORDER BY conname`,
      [
        [
          'buckets_lifecycle_configuration_pair_check',
          'buckets_lifecycle_configuration_shape_check',
          'buckets_lifecycle_configuration_standard_only_check',
        ],
      ]
    )
    expect(constraints.rows).toEqual([
      {
        conname: 'buckets_lifecycle_configuration_pair_check',
        convalidated: true,
      },
      {
        conname: 'buckets_lifecycle_configuration_shape_check',
        convalidated: true,
      },
      {
        conname: 'buckets_lifecycle_configuration_standard_only_check',
        convalidated: true,
      },
    ])
  })

  it('rejects direct bucket-column writes and empty policies at the database boundary', async () => {
    const transaction = await helper.database.connection.transaction()
    try {
      await expect(
        transaction.query(
          `UPDATE storage.buckets
           SET lifecycle_configuration = $2::jsonb,
               lifecycle_configuration_generation = $3::uuid
           WHERE id = $1`,
          [bucketId, JSON.stringify({ rules }), randomUUID()]
        )
      ).rejects.toMatchObject({ code: '42501' })
    } finally {
      await transaction.rollback()
    }

    await expect(putLifecycleConfiguration({ rules: [] })).rejects.toMatchObject({
      code: 'DatabaseError',
      originalError: {
        code: '23514',
        constraint: 'buckets_lifecycle_configuration_shape_check',
      },
    })
  })

  it('rejects lifecycle policy state on service-role bucket inserts', async () => {
    const transaction = await helper.database.connection.transaction()

    try {
      await helper.database.connection.setScope(transaction)
      await expect(
        transaction.query(
          `INSERT INTO storage.buckets (
             id,
             name,
             lifecycle_configuration,
             lifecycle_configuration_generation
           )
           VALUES ($1, $1, $2::jsonb, $3::uuid)`,
          [`${bucketId}-service-role-insert`, JSON.stringify({ rules }), randomUUID()]
        )
      ).rejects.toMatchObject({ code: '42501' })
    } finally {
      await transaction.rollback()
    }
  })

  it('requires protected lifecycle defaults on non-service-role bucket inserts', async () => {
    const transaction = await helper.database.connection.transaction()

    try {
      await expect(
        transaction.query(
          `INSERT INTO storage.buckets (
             id,
             name,
             lifecycle_configuration,
             lifecycle_configuration_generation
           )
           VALUES ($1, $1, $2::jsonb, $3::uuid)`,
          [`${bucketId}-protected-insert`, JSON.stringify({ rules }), randomUUID()]
        )
      ).rejects.toMatchObject({ code: '42501' })
    } finally {
      await transaction.rollback()
    }
  })

  it.each([
    'ANALYTICS',
    'VECTOR',
  ])('rejects lifecycle policy state for a resolved %s bucket', async (type) => {
    await helper.database.connection.query(`UPDATE storage.buckets SET type = $2 WHERE id = $1`, [
      bucketId,
      type,
    ])

    await expect(putLifecycleConfiguration({ rules })).rejects.toMatchObject({
      code: 'InvalidRequest',
      httpStatusCode: 400,
      message: 'Versioning and lifecycle are only supported for Standard buckets',
    })

    await expect(
      helper.database.connection.query(
        `UPDATE storage.buckets
         SET lifecycle_configuration = '{"rules": []}'::jsonb,
             lifecycle_configuration_generation = $2::uuid
         WHERE id = $1`,
        [bucketId, randomUUID()]
      )
    ).rejects.toMatchObject({ code: '0A000' })
  })
})
