import { ERRORS } from '@internal/errors'
import { getConfig } from '../../config'
import type { Database } from '../database'

const { storageLifecycleEnabled } = getConfig()

export function assertLifecycleApiEnabled(bucketId: string): void {
  if (!storageLifecycleEnabled) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object lifecycles')
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

export async function assertLifecycleWriteReady(
  database: Pick<Database, 'hasMigration'>,
  bucketId: string
): Promise<void> {
  assertLifecycleApiEnabled(bucketId)
  await assertLifecycleSchemaReady(database, bucketId)
}
