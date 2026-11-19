import { randomUUID } from 'node:crypto'
import * as tenants from '@internal/database'
import {
  closeMultitenantPg,
  LifecycleTenantStorePg,
  multitenantPgExecutor,
} from '@internal/database'
import { runMultitenantMigrations } from '@internal/database/migrations'
import { logSchema } from '@internal/monitoring'
import { StoragePgDB } from '@storage/database'
import { LifecycleBucketRun, LifecycleTenantRun } from '@storage/events/lifecycle-rules'
import { Storage } from '@storage/storage'
import { Client } from 'pg'
import * as config from '../config'
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
    vi.spyOn(config, 'getConfig').mockReturnValue({ ...config.getConfig(), isMultitenant: true })
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
      vi.spyOn(
        LifecycleTenantRun as unknown as { createStorage: () => Promise<Storage> },
        'createStorage'
      ).mockResolvedValue(storage)
      const send = vi.spyOn(LifecycleBucketRun, 'send').mockResolvedValue('job')
      const job = {
        data: {
          tenant: { ref: tenantId, host: 'localhost' },
          centralClaim: { claimId: claim.claimId },
        },
        startedOn: new Date(Date.now() - (mode === 'expired' ? 185_000 : 0)),
      } as never

      const budget = config.getConfig().storageLifecycleJobBudgetMs
      const startedAt = Date.now()
      await LifecycleTenantRun.handle(job)
      const finishedAt = Date.now()
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
      if (mode === 'stale feature cache') {
        expect(send).not.toHaveBeenCalled()
        expect(result.rows[0].next_dispatch_at.getTime() - Date.now()).toBeGreaterThan(299_000)
        expect(result.rows[0].next_dispatch_at.getTime() - Date.now()).toBeLessThanOrEqual(300_000)
      } else {
        expect(send).toHaveBeenCalledWith({
          tenant: { ref: tenantId, host: 'localhost' },
          bucketId,
        })
      }
    }
  )

  it('keeps the policy when the central tenant is missing and enrollment fails', async () => {
    const { helper, bucketId } = fixtures
    await fixtures.setStatus(bucketId, 'ENABLED')
    const database = new StoragePgDB(helper.database.connection, { tenantId, host: 'localhost' })
    const storage = new Storage(helper.adapter, database, helper.storage.location)
    vi.spyOn(config, 'getConfig').mockReturnValue({ ...config.getConfig(), isMultitenant: true })
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
