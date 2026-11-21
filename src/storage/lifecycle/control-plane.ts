import { LifecycleTenantStorePg, multitenantPgExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { getConfig } from '../../config'
import type { Database } from '../database'

export async function wakeLifecycleTenant(
  database: Pick<Database, 'tenantId' | 'reqId' | 'sbReqId'>
): Promise<void> {
  if (!getConfig().isMultitenant) return

  const { tenantId, reqId, sbReqId } = database
  try {
    await new LifecycleTenantStorePg(multitenantPgExecutor).wakeTenant(
      tenantId,
      AbortSignal.timeout(5000)
    )
  } catch (error) {
    logSchema.warning(logger, '[Lifecycle] Failed to wake lifecycle tenant after commit', {
      type: 'event',
      tenantId,
      project: tenantId,
      reqId,
      sbReqId,
      error,
    })
  }
}
