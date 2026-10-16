import { tenantHasFeature } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { getConfig } from '../../config'
import type { Database } from '../database'

export function isLifecycleExecutionEnabled(): boolean {
  const { isMultitenant, storageLifecycleEnabled } = getConfig()
  return isMultitenant || storageLifecycleEnabled
}

export async function isLifecycleEnabledForTenant(tenantId: string): Promise<boolean> {
  const { isMultitenant, storageLifecycleEnabled } = getConfig()
  return isMultitenant ? tenantHasFeature(tenantId, 'objectVersioning') : storageLifecycleEnabled
}

export function assertLifecycleExecutionEnabled(bucketId: string): void {
  if (!isLifecycleExecutionEnabled()) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object lifecycle execution')
  }
}

export async function assertLifecycleSchemaReady(
  database: Pick<Database, 'hasMigration'>,
  bucketId: string
): Promise<void> {
  if (!(await database.hasMigration('bucket-lifecycle-configuration'))) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object lifecycles')
  }
}
