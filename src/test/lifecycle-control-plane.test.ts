import { randomUUID } from 'node:crypto'
import {
  closeMultitenantPg,
  LifecycleTenantStorePg,
  multitenantPgExecutor,
} from '@internal/database'
import { runMultitenantMigrations } from '@internal/database/migrations'
import { logSchema } from '@internal/monitoring'
import { StoragePgDB } from '@storage/database'
import { Storage } from '@storage/storage'
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
    const failure = new Error('control database unavailable')
    vi.spyOn(LifecycleTenantStorePg.prototype, 'wakeTenant').mockRejectedValue(failure)

    await expect(storage.putBucketLifecycle(bucketId, policy)).resolves.toEqual(policy)
    await expect(database.findLifecycleBucket(bucketId)).resolves.toMatchObject({
      lifecycle_configuration: policy,
    })
    expect(warning).toHaveBeenCalledWith(
      expect.anything(),
      expect.stringContaining('wake lifecycle tenant after commit'),
      expect.objectContaining({ error: failure, tenantId })
    )
  })
})
