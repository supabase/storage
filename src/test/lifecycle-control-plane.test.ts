import { randomUUID } from 'node:crypto'
import { DeleteQueueCommand } from '@aws-sdk/client-sqs'
import * as tenants from '@internal/database'
import {
  closeMultitenantPg,
  LifecycleTenantStorePg,
  multitenantPgExecutor,
} from '@internal/database'
import { runMultitenantMigrations } from '@internal/database/migrations'
import { logSchema } from '@internal/monitoring'
import { sqs } from '@internal/queue/adapters/sqs'
import { createQueueManager, defineQueues, queue } from '@internal/queue/manager'
import { StoragePgDB } from '@storage/database'
import * as eventBase from '@storage/events/base'
import {
  LifecycleBucketRun,
  LifecycleTenantRun,
  LifecycleTenantRunHandler,
} from '@storage/events/lifecycle-rules'
import { deadLetterQueueOf, QUEUES, storageQueues } from '@storage/events/registry'
import { Storage } from '@storage/storage'
import { Client } from 'pg'
import * as config from '../config'
import { queueUrlOf, rawSqsClient, sqsClientConfig, sqsEndpoint } from './queue/sqs-helpers'
import { useLifecycleStorage } from './utils/lifecycle-versioning'

describe('lifecycle policy enrollment across tenant and control databases', () => {
  const fixtures = useLifecycleStorage('lifecycle-enrollment')
  const tenantId = `lifecycle-enrollment-${randomUUID()}`
  const policy = {
    rules: [
      {
        status: 'Enabled' as const,
        filter: {},
        noncurrentVersionExpiration: { noncurrentDays: 1 },
      },
    ],
  }

  beforeAll(async () => {
    await runMultitenantMigrations()
    await multitenantPgExecutor.query({
      text: `INSERT INTO public.tenants
        (id, anon_key, database_url, jwt_secret, service_key, feature_object_versioning)
        VALUES ($1, 'fixture', 'fixture', 'fixture', 'fixture', false)`,
      values: [tenantId],
    })
  })

  afterAll(async () => {
    try {
      await multitenantPgExecutor.query({
        text: 'DELETE FROM public.tenants WHERE id = $1',
        values: [tenantId],
      })
    } finally {
      await closeMultitenantPg()
    }
  })

  beforeEach(async () => {
    await multitenantPgExecutor.query({
      text: 'UPDATE public.tenants SET feature_object_versioning = false WHERE id = $1',
      values: [tenantId],
    })
    await multitenantPgExecutor.query({
      text: 'DELETE FROM public.lifecycle_tenants WHERE tenant_id = $1',
      values: [tenantId],
    })
  })

  it('enrolls a flagged tenant and keeps a committed policy when the post-commit wake fails', async () => {
    const { helper, bucketId } = fixtures
    await fixtures.setStatus(bucketId, 'ENABLED')
    const database = new StoragePgDB(helper.database.connection, { tenantId, host: 'localhost' })
    const storage = new Storage(helper.adapter, database, helper.storage.location)
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      isMultitenant: true,
      storageVersioningEnabled: false,
    })
    vi.spyOn(tenants, 'tenantHasFeature').mockResolvedValue(true)
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    const enrollment = () =>
      multitenantPgExecutor.query({
        text: 'SELECT 1 FROM public.lifecycle_tenants WHERE tenant_id = $1',
        values: [tenantId],
      })

    await expect(enrollment()).resolves.toMatchObject({ rowCount: 0 })
    await multitenantPgExecutor.query({
      text: 'UPDATE public.tenants SET feature_object_versioning = true WHERE id = $1',
      values: [tenantId],
    })
    // The feature trigger, not the PUT, enrolls the tenant.
    await expect(enrollment()).resolves.toMatchObject({ rowCount: 1 })
    await multitenantPgExecutor.query({
      text: 'DELETE FROM public.lifecycle_tenants WHERE tenant_id = $1',
      values: [tenantId],
    })
    const failure = new Error('control database unavailable')
    vi.spyOn(LifecycleTenantStorePg.prototype, 'wakeTenant').mockRejectedValue(failure)

    await expect(storage.putBucketLifecycle(bucketId, policy)).resolves.toEqual(policy)
    await expect(database.findLifecycleBucket(bucketId)).resolves.toMatchObject({
      lifecycle_configuration: policy,
    })
    await expect(enrollment()).resolves.toMatchObject({ rowCount: 0 })
    expect(warning).toHaveBeenCalledWith(
      expect.anything(),
      expect.stringContaining('wake lifecycle tenant after commit'),
      expect.objectContaining({ error: failure, tenantId })
    )
  })

  it.each(['active', 'expired', 'stale feature cache'] as const)(
    'dispatches against the real central store with %s',
    async (mode) => {
      const { helper, bucketId } = fixtures
      await fixtures.setStatus(bucketId, 'ENABLED')
      await helper.database.putLifecycleConfiguration(bucketId, policy)
      await helper.database.connection.query({
        text: 'UPDATE storage.bucket_lifecycle_states SET failure_count = 3 WHERE bucket_id = $1',
        values: [bucketId],
      })
      await multitenantPgExecutor.query({
        text: 'UPDATE public.tenants SET feature_object_versioning = true WHERE id = $1',
        values: [tenantId],
      })
      const store = new LifecycleTenantStorePg(multitenantPgExecutor)
      const claims = await store.claimDueTenants(100, 180_000)
      const claim = claims.find((entry) => entry.tenantId === tenantId)!
      expect(claim).toBeDefined()
      const database = new StoragePgDB(helper.database.connection.asSuperUser(), {
        tenantId,
        host: 'localhost',
      })
      const storage = new Storage(helper.adapter, database, helper.storage.location)
      vi.spyOn(config, 'getConfig').mockReturnValue({
        ...config.getConfig(),
        isMultitenant: true,
        storageVersioningEnabled: false,
      })
      vi.spyOn(tenants, 'tenantHasFeature').mockResolvedValue(mode !== 'stale feature cache')
      vi.spyOn(eventBase, 'createStorage').mockResolvedValue(storage)
      const send = vi.spyOn(LifecycleBucketRun, 'batchSend').mockResolvedValue()
      const job = {
        message: {
          data: {
            tenant: { ref: tenantId, host: 'localhost' },
            centralClaim: {
              claimId: claim.claimId,
              claimUntil:
                mode === 'expired'
                  ? new Date(Date.now() - 5_000).toISOString()
                  : claim.claimUntil.toISOString(),
            },
          },
        },
        signal: new AbortController().signal,
      } as never

      const budget = config.getConfig().storageLifecycleJobBudgetMs
      // The next dispatch is stamped by the tenant database, whose clock can differ from the host's.
      const tenantDatabaseNow = async () => {
        const { rows } = await helper.database.connection.query('SELECT clock_timestamp() AS now')
        return (rows[0].now as Date).getTime()
      }
      const startedAt = await tenantDatabaseNow()
      await new LifecycleTenantRunHandler().handle(job)
      const finishedAt = await tenantDatabaseNow()
      const result = await multitenantPgExecutor.query({
        text: `SELECT claim_id, next_dispatch_at, last_success_at, failure_count,
            max_bucket_failure_count, bucket_status_observed_at
          FROM public.lifecycle_tenants WHERE tenant_id = $1`,
        values: [tenantId],
      })
      if (mode === 'expired') {
        expect(result.rows[0].claim_id).toBe(claim.claimId)
        expect(result.rows[0].last_success_at).toBeNull()
      } else {
        expect(result.rows[0].claim_id).toBeNull()
        expect(result.rows[0].last_success_at).not.toBeNull()
        expect(result.rows[0].next_dispatch_at).toBeInstanceOf(Date)
        expect(result.rows[0].max_bucket_failure_count).toBe(3)
        if (mode === 'active') {
          const nextDispatchAt = result.rows[0].next_dispatch_at.getTime()
          expect(nextDispatchAt).toBeGreaterThanOrEqual(startedAt + budget)
          expect(nextDispatchAt).toBeLessThanOrEqual(finishedAt + budget)
        }
        expect(result.rows[0].bucket_status_observed_at).toBeInstanceOf(Date)
      }
      if (mode === 'stale feature cache' || mode === 'expired') {
        expect(send).not.toHaveBeenCalled()
      }
      if (mode === 'stale feature cache') {
        expect(result.rows[0].next_dispatch_at.getTime() - Date.now()).toBeGreaterThan(299_000)
        expect(result.rows[0].next_dispatch_at.getTime() - Date.now()).toBeLessThanOrEqual(300_000)
      } else if (mode === 'active') {
        expect(send).toHaveBeenCalledWith(
          [
            {
              tenant: { ref: tenantId, host: 'localhost' },
              bucketId,
              dispatchId: expect.any(String),
            },
          ],
          { signal: expect.any(AbortSignal) }
        )
      }
    }
  )

  it.skipIf(sqsEndpoint === undefined)(
    'fences duplicate SQS deliveries of a tenant claim',
    async () => {
      const { helper, bucketId } = fixtures
      await fixtures.setStatus(bucketId, 'ENABLED')
      await helper.database.putLifecycleConfiguration(bucketId, policy)
      const store = new LifecycleTenantStorePg(multitenantPgExecutor)
      await store.wakeTenant(tenantId)
      const [claim] = await store.claimDueTenants(1, 180_000)
      const storage = new Storage(
        helper.adapter,
        new StoragePgDB(helper.database.connection.asSuperUser(), {
          tenantId,
          host: 'localhost',
        }),
        helper.storage.location
      )
      vi.spyOn(config, 'getConfig').mockReturnValue({
        ...config.getConfig(),
        isMultitenant: true,
        storageVersioningEnabled: false,
      })
      vi.spyOn(tenants, 'tenantHasFeature').mockResolvedValue(true)
      vi.spyOn(eventBase, 'createStorage').mockResolvedValue(storage)
      const sendBucket = vi.spyOn(LifecycleBucketRun, 'batchSend').mockResolvedValue()
      const start = vi.spyOn(LifecycleTenantStorePg.prototype, 'startTenantDispatch')
      const complete = vi.spyOn(LifecycleTenantStorePg.prototype, 'completeTenantDispatch')
      const prefix = `lc-${randomUUID().slice(0, 8)}-`
      const name = QUEUES.lifecycleTenantRun
      const queues = defineQueues({
        [name]: queue(LifecycleTenantRun, storageQueues.get(name)?.options),
      })
      const manager = createQueueManager(
        sqs({ client: sqsClientConfig(), prefix, waitTimeSeconds: 0 }),
        {
          queues,
          handlers: [new LifecycleTenantRunHandler()],
          pollIdleIntervalMs: 50,
        }
      )
      const raw = rawSqsClient()
      try {
        await manager.start()
        const payload = {
          tenant: { ref: tenantId, host: 'localhost' },
          centralClaim: {
            claimId: claim.claimId,
            claimUntil: claim.claimUntil.toISOString(),
          },
        }
        await manager.produce([new LifecycleTenantRun(payload), new LifecycleTenantRun(payload)])
        await vi.waitFor(() => expect(start).toHaveBeenCalledTimes(2), { timeout: 10_000 })
        await vi.waitFor(() => expect(complete).toHaveBeenCalledOnce(), { timeout: 10_000 })
        expect(sendBucket).toHaveBeenCalledExactlyOnceWith(
          [{ tenant: payload.tenant, bucketId, dispatchId: expect.any(String) }],
          {
            signal: expect.any(AbortSignal),
          }
        )
      } finally {
        await manager.close()
        for (const queueName of [name, deadLetterQueueOf(name)]) {
          const queueUrl = await queueUrlOf(raw, `${prefix}${queueName}`)
          await raw.send(new DeleteQueueCommand({ QueueUrl: queueUrl }))
        }
        raw.destroy()
      }
    }
  )

  it('enrolls a global-flag tenant before saving so a failed post-commit wake still leaves it scheduled', async () => {
    const { helper, bucketId } = fixtures
    await fixtures.setStatus(bucketId, 'ENABLED')
    const database = new StoragePgDB(helper.database.connection, { tenantId, host: 'localhost' })
    const storage = new Storage(helper.adapter, database, helper.storage.location)
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      isMultitenant: true,
      storageVersioningEnabled: true,
    })
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    const failure = new Error('control database unavailable')
    vi.spyOn(LifecycleTenantStorePg.prototype, 'wakeTenant').mockRejectedValue(failure)

    await expect(storage.putBucketLifecycle(bucketId, policy)).resolves.toEqual(policy)
    await expect(database.findLifecycleBucket(bucketId)).resolves.toMatchObject({
      lifecycle_configuration: policy,
    })
    const registry = await multitenantPgExecutor.query({
      text: 'SELECT 1 FROM public.lifecycle_tenants WHERE tenant_id = $1',
      values: [tenantId],
    })
    expect(registry.rowCount).toBe(1)
    expect(warning).toHaveBeenCalledWith(
      expect.anything(),
      expect.stringContaining('wake lifecycle tenant after commit'),
      expect.objectContaining({ error: failure, tenantId })
    )
  })

  it('rejects the policy without saving when global-flag pre-enrollment fails for a missing central tenant', async () => {
    const { helper, bucketId } = fixtures
    await fixtures.setStatus(bucketId, 'ENABLED')
    const database = new StoragePgDB(helper.database.connection, { tenantId, host: 'localhost' })
    const storage = new Storage(helper.adapter, database, helper.storage.location)
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      isMultitenant: true,
      storageVersioningEnabled: true,
    })
    await multitenantPgExecutor.query({
      text: 'DELETE FROM public.tenants WHERE id = $1',
      values: [tenantId],
    })

    try {
      await expect(storage.putBucketLifecycle(bucketId, policy)).rejects.toMatchObject({
        code: '23503',
      })
      expect((await database.findLifecycleBucket(bucketId)).lifecycle_configuration).toBeNull()
      const registry = await multitenantPgExecutor.query({
        text: 'SELECT 1 FROM public.lifecycle_tenants WHERE tenant_id = $1',
        values: [tenantId],
      })
      expect(registry.rowCount).toBe(0)
    } finally {
      await multitenantPgExecutor.query({
        text: `INSERT INTO public.tenants
          (id, anon_key, database_url, jwt_secret, service_key, feature_object_versioning)
          VALUES ($1, 'fixture', 'fixture', 'fixture', 'fixture', false)`,
        values: [tenantId],
      })
    }
  })

  it('keeps the policy when the central tenant is missing and post-commit wake fails under per-tenant flags', async () => {
    const { helper, bucketId } = fixtures
    await fixtures.setStatus(bucketId, 'ENABLED')
    const database = new StoragePgDB(helper.database.connection, { tenantId, host: 'localhost' })
    const storage = new Storage(helper.adapter, database, helper.storage.location)
    vi.spyOn(config, 'getConfig').mockReturnValue({
      ...config.getConfig(),
      isMultitenant: true,
      storageVersioningEnabled: false,
    })
    vi.spyOn(tenants, 'tenantHasFeature').mockResolvedValue(true)
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    await multitenantPgExecutor.query({
      text: 'DELETE FROM public.tenants WHERE id = $1',
      values: [tenantId],
    })

    try {
      await expect(storage.putBucketLifecycle(bucketId, policy)).resolves.toEqual(policy)
      expect((await database.findLifecycleBucket(bucketId)).lifecycle_configuration).toEqual(policy)
      expect(warning).toHaveBeenCalledWith(
        expect.anything(),
        expect.stringContaining('wake lifecycle tenant after commit'),
        expect.objectContaining({ error: expect.objectContaining({ code: '23503' }), tenantId })
      )
      const registry = await multitenantPgExecutor.query({
        text: 'SELECT 1 FROM public.lifecycle_tenants WHERE tenant_id = $1',
        values: [tenantId],
      })
      expect(registry.rowCount).toBe(0)
    } finally {
      await multitenantPgExecutor.query({
        text: `INSERT INTO public.tenants
          (id, anon_key, database_url, jwt_secret, service_key, feature_object_versioning)
          VALUES ($1, 'fixture', 'fixture', 'fixture', 'fixture', false)`,
        values: [tenantId],
      })
    }
  })

  it.each(['put', 'delete', 'enable versioning'] as const)(
    'commits %s before attempting a wake',
    async (method) => {
      const { helper, bucketId } = fixtures
      await fixtures.setStatus(bucketId, method === 'enable versioning' ? 'SUSPENDED' : 'ENABLED')
      if (method === 'delete') await helper.database.putLifecycleConfiguration(bucketId, policy)
      const database = new StoragePgDB(helper.database.connection, { tenantId, host: 'localhost' })
      const storage = new Storage(helper.adapter, database, helper.storage.location)
      vi.spyOn(config, 'getConfig').mockReturnValue({ ...config.getConfig(), isMultitenant: true })
      let observed: unknown
      const wake = vi
        .spyOn(LifecycleTenantStorePg.prototype, 'wakeTenant')
        .mockImplementation(async () => {
          const row = await helper.database.findLifecycleBucket(bucketId)
          observed =
            method === 'enable versioning' ? row.versioning_status : row.lifecycle_configuration
        })

      if (method === 'put') await storage.putBucketLifecycle(bucketId, policy)
      else if (method === 'delete') await storage.deleteBucketLifecycle(bucketId)
      else await storage.updateBucket(bucketId, { versioning_status: 'ENABLED' })

      expect(wake).toHaveBeenCalledOnce()
      expect(observed).toEqual(method === 'put' ? policy : method === 'delete' ? null : 'ENABLED')
    }
  )

  it('rolls back a blocked policy write after request abort', async () => {
    const { helper, bucketId } = fixtures
    await fixtures.setStatus(bucketId, 'ENABLED')
    const blocker = await helper.database.connection.transaction()
    const observer = new Client({ connectionString: config.getConfig().databaseURL })
    await observer.connect()
    const controller = new AbortController()
    const connection = helper.database.connection.asSuperUser()
    connection.setAbortSignal(controller.signal)
    const database = new StoragePgDB(connection, {
      tenantId: helper.database.tenantId,
      host: 'localhost',
    })
    const storage = new Storage(helper.adapter, database, helper.storage.location)
    let operation: Promise<unknown> | undefined
    try {
      await blocker.query({
        text: 'SELECT id FROM storage.buckets WHERE id = $1 FOR UPDATE',
        values: [bucketId],
      })
      operation = storage.putBucketLifecycle(bucketId, policy)
      // Prevent an unhandled rejection while waiting for the query to block.
      const outcome = operation.then(
        () => null,
        (error: unknown) => error
      )
      await vi.waitFor(async () => {
        const { rows } = await observer.query(`
            SELECT EXISTS (SELECT 1 FROM pg_stat_activity
              WHERE wait_event_type = 'Lock' AND query LIKE '%storage.buckets%FOR UPDATE%') AS blocked
          `)
        expect(rows[0].blocked).toBe(true)
      })
      controller.abort()
      const error = await outcome
      expect(error).toBeInstanceOf(Error)
      expect(error).not.toMatchObject({ code: 'DatabaseTimeout' })
    } finally {
      await blocker.rollback()
      connection.dispose()
      await observer.end()
      await operation?.catch(() => undefined)
    }
    expect((await helper.database.findLifecycleBucket(bucketId)).lifecycle_configuration).toBeNull()
  })
})
