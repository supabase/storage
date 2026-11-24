import { LifecycleTenantStorePg, multitenantPgExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { getConfig } from '../../config'
import type { Database } from '../database'

const LIFECYCLE_CONTROL_PLANE_TIMEOUT_MS = 5000
export const LIFECYCLE_PRE_COMMIT_WAKE_DELAY_MS = 5 * 60_000

// Durably enroll the tenant and pull its next revisit to at most 5m before saving the policy,
// so a failed post-commit wake (or crash after commit) still picks up the change promptly
// instead of waiting for the 30-day idle revisit cap.
export async function prepareLifecycleTenantWake(
  database: Pick<Database, 'tenantId'>
): Promise<void> {
  if (!getConfig().isMultitenant) return

  await new LifecycleTenantStorePg(multitenantPgExecutor).wakeTenant(
    database.tenantId,
    AbortSignal.timeout(LIFECYCLE_CONTROL_PLANE_TIMEOUT_MS),
    LIFECYCLE_PRE_COMMIT_WAKE_DELAY_MS
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
