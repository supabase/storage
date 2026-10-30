import { tenantHasFeature } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { getConfig } from '../../config'
import type { Database } from '../database'

export async function isLifecycleEnabledForTenant(tenantId: string): Promise<boolean> {
  const { isMultitenant, storageVersioningEnabled } = getConfig()
  return (
    storageVersioningEnabled ||
    (isMultitenant && (await tenantHasFeature(tenantId, 'objectVersioning')))
  )
}

export async function assertTenantLifecycleApiEnabled(
  bucketId: string,
  database: Pick<Database, 'tenantId'>
): Promise<void> {
  if (!(await isLifecycleEnabledForTenant(database.tenantId))) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object lifecycles')
  }
}

export async function assertLifecycleSchemaReady(
  database: Pick<Database, 'hasMigration'>,
  bucketId: string
): Promise<void> {
  if (!(await database.hasMigration('lifecycle-writer-coordination'))) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object lifecycles')
  }
}

export async function assertLifecycleWriteReady(
  database: Pick<Database, 'hasMigration' | 'tenantId'>,
  bucketId: string
): Promise<void> {
  await assertTenantLifecycleApiEnabled(bucketId, database)
  await assertLifecycleSchemaReady(database, bucketId)
}
