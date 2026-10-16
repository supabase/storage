import { LifecycleTenantStorePg, multitenantPgExecutor } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import { getConfig } from '../../config'
import type { Database } from '../database'
import { isLifecycleEnabledForTenant } from './guards'

export async function assertTenantLifecycleApiEnabled(bucketId: string, database: Database) {
  if (!(await isLifecycleEnabledForTenant(database.tenantId))) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object lifecycle for this tenant')
  }
}

export async function withLifecycleTransaction<T>(
  database: Database,
  mutation: (database: Database) => Promise<T>
): Promise<T> {
  const config = getConfig()
  const tenantId = database.tenantId
  const deadlineSignal = AbortSignal.timeout(30_000)
  const requestSignal = database.connection.getAbortSignal()
  const tenantStore = config.isMultitenant
    ? new LifecycleTenantStorePg(multitenantPgExecutor)
    : undefined
  const operationSignal = requestSignal
    ? AbortSignal.any([requestSignal, deadlineSignal])
    : deadlineSignal
  database.connection.setAbortSignal(operationSignal)

  try {
    operationSignal.throwIfAborted()
    const result = await database.withTransaction(async (db) => {
      const result = await mutation(db)
      operationSignal.throwIfAborted()
      return result
    })
    // Committed work gets its own wake budget, independent of request cancellation.
    try {
      await tenantStore?.wakeTenant(tenantId, AbortSignal.timeout(5000))
    } catch (error) {
      logSchema.warning(logger, '[Lifecycle] Failed to wake lifecycle tenant after commit', {
        type: 'event',
        tenantId,
        project: tenantId,
        error,
      })
    }
    return result
  } catch (error) {
    if (deadlineSignal.aborted && !requestSignal?.aborted) {
      throw ERRORS.DatabaseTimeout(error instanceof Error ? error : undefined)
    }
    throw error
  } finally {
    database.connection.setAbortSignal(requestSignal)
  }
}
