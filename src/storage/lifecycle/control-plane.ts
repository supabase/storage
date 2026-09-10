import { LifecycleTenantStorePg, multitenantPgExecutor, tenantHasFeature } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import { getConfig } from '../../config'
import type { Database } from '../database'
import { assertLifecycleApiEnabled } from './guards'

export async function assertTenantLifecycleApiEnabled(bucketId: string, tenantId: string) {
  assertLifecycleApiEnabled(bucketId)
  if (!(await tenantHasFeature(tenantId, 'objectVersioning'))) {
    throw ERRORS.FeatureNotEnabled(bucketId, 'object lifecycle for this tenant')
  }
}

export async function withLifecycleConfigurationTransaction<T>(
  database: Database,
  tenantId: string,
  mutation: (database: Database) => Promise<T>
): Promise<T> {
  const config = getConfig()
  const deadlineSignal = AbortSignal.timeout(
    config.storageLifecycleConfigurationTransactionTimeoutMs
  )
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
    await tenantStore?.wakeTenant(tenantId, operationSignal)
    operationSignal.throwIfAborted()
    const result = await database.withTransaction(mutation, { deadlineSignal: operationSignal })
    try {
      // The dispatcher may have consumed the first wake before this commit.
      // Give committed work its own wake budget, independent of request cancellation.
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
  } finally {
    database.connection.setAbortSignal(requestSignal)
  }
}
