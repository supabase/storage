import { LifecycleTenantStorePg, multitenantPgExecutor } from '@internal/database'
import { ERRORS, StorageBackendError } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import { getConfig } from '../../config'
import type { Database } from '../database'
import {
  LIFECYCLE_POST_COMMIT_WAKE_TIMEOUT_MS,
  LIFECYCLE_PRE_COMMIT_WAKE_DELAY_MS,
  LIFECYCLE_PRE_COMMIT_WAKE_TIMEOUT_MS,
} from './timings'

export { LIFECYCLE_PRE_COMMIT_WAKE_DELAY_MS }

// Durably enroll the tenant and pull its next revisit to at most 5m before committing the
// authorized mutation, so a failed post-commit wake (or crash after commit) still picks up the
// change promptly instead of waiting for the 30-day idle revisit cap.
export async function prepareLifecycleTenantWake(
  database: Pick<Database, 'tenantId'> & {
    connection?: Pick<Database['connection'], 'getAbortSignal'>
  }
): Promise<void> {
  const requestSignal = database.connection?.getAbortSignal()
  requestSignal?.throwIfAborted()
  if (!getConfig().isMultitenant) return

  const timeoutSignal = AbortSignal.timeout(LIFECYCLE_PRE_COMMIT_WAKE_TIMEOUT_MS)
  const signal = requestSignal ? AbortSignal.any([requestSignal, timeoutSignal]) : timeoutSignal

  try {
    await new LifecycleTenantStorePg(multitenantPgExecutor).wakeTenant(
      database.tenantId,
      signal,
      LIFECYCLE_PRE_COMMIT_WAKE_DELAY_MS
    )
  } catch (error) {
    if (requestSignal?.aborted) throw requestSignal.reason ?? error
    // Wrap control-plane failures so tenant-DB transaction error mapping (mapPgError)
    // cannot reinterpret a central DatabaseError (e.g. FK 23503) as a 4xx tenant error.
    if (error instanceof StorageBackendError) throw error
    throw ERRORS.InternalError(error instanceof Error ? error : new Error(String(error)))
  }

  requestSignal?.throwIfAborted()
}

export async function wakeLifecycleTenant(
  database: Pick<Database, 'tenantId' | 'reqId' | 'sbReqId'>
): Promise<void> {
  if (!getConfig().isMultitenant) return

  const { tenantId, reqId, sbReqId } = database
  try {
    await new LifecycleTenantStorePg(multitenantPgExecutor).wakeTenant(
      tenantId,
      AbortSignal.timeout(LIFECYCLE_POST_COMMIT_WAKE_TIMEOUT_MS)
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
