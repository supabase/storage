import { LifecycleTenantStorePg, multitenantPgExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { getConfig } from '../../config'
import type { Database } from '../database'

const LIFECYCLE_CONTROL_PLANE_TIMEOUT_MS = 5000

// With the global versioning flag, tenants without feature_object_versioning are not enrolled
// by the tenants table trigger. Enroll before saving so a failed post-commit wake only delays
// execution instead of leaving a policy permanently unscheduled.
export async function ensureLifecycleTenantEnrolled(
  database: Pick<Database, 'tenantId'>
): Promise<void> {
  const { isMultitenant, storageVersioningEnabled } = getConfig()
  if (!isMultitenant || !storageVersioningEnabled) return

  await new LifecycleTenantStorePg(multitenantPgExecutor).enrollTenant(
    database.tenantId,
    AbortSignal.timeout(LIFECYCLE_CONTROL_PLANE_TIMEOUT_MS)
  )
}

export async function wakeLifecycleTenant(
  database: Pick<Database, 'tenantId' | 'reqId' | 'sbReqId'>
): Promise<void> {
  if (!getConfig().isMultitenant) return

  const { tenantId, reqId, sbReqId } = database
  try {
    await new LifecycleTenantStorePg(multitenantPgExecutor).wakeTenant(
      tenantId,
      AbortSignal.timeout(LIFECYCLE_CONTROL_PLANE_TIMEOUT_MS)
    )
  } catch (error) {
    logSchema.warning(logger, '[LifecycleRule] Failed to wake lifecycle tenant after commit', {
      type: 'lifecycle-rule',
      tenantId,
      project: tenantId,
      reqId,
      sbReqId,
      error,
    })
  }
}
