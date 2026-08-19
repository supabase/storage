import { ERRORS } from '@internal/errors'
import { getConfig } from '../../config'
import type { Database } from '../database'

interface LifecycleStorageContext {
  db: Pick<Database, 'hasMigration'>
}

export function assertLifecycleApiEnabled(bucketId: string): void {
  if (!getConfig().storageLifecycleEnabled) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object lifecycle')
  }
}

export async function assertLifecycleSchemaReady(
  storage: LifecycleStorageContext,
  bucketId: string
): Promise<void> {
  if (!(await storage.db.hasMigration('bucket-lifecycle-configuration'))) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object lifecycle schema')
  }
}
