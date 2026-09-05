import { tenantHasFeature } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { getConfig } from '../../config'
import type { Database } from '../database'
import type { BucketType } from '../limits'

interface VersioningStorageContext {
  db: Pick<Database, 'hasMigration' | 'tenantId'>
}

function assertVersioningApiEnabled(bucketId: string): void {
  const { isMultitenant, storageVersioningEnabled } = getConfig()

  if (!isMultitenant && !storageVersioningEnabled) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object versioning')
  }
}

async function assertVersioningTenantEnabled(
  storage: VersioningStorageContext,
  bucketId: string
): Promise<void> {
  const { isMultitenant } = getConfig()

  if (isMultitenant && !(await tenantHasFeature(storage.db.tenantId, 'objectVersioning'))) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object versioning tenant flag')
  }
}

async function assertVersioningSchemaReady(
  storage: VersioningStorageContext,
  bucketId: string
): Promise<void> {
  if (!(await storage.db.hasMigration('unlock-object-versioning'))) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object versioning schema')
  }
}

function assertVersioningStandardBucketOnly(bucketType: BucketType): void {
  if (bucketType !== 'STANDARD') {
    throw ERRORS.VersioningRequiresStandardBucket()
  }
}

export async function assertVersioningEnabled(
  storage: VersioningStorageContext,
  bucketId: string,
  bucketType: BucketType
): Promise<void> {
  assertVersioningApiEnabled(bucketId)
  assertVersioningStandardBucketOnly(bucketType)
  await assertVersioningTenantEnabled(storage, bucketId)
  await assertVersioningSchemaReady(storage, bucketId)
}
