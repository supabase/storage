import { ErrorCode, StorageBackendError } from '@internal/errors'
import { getConfig } from '../../../config'
import { logger, logSchema } from '../../monitoring'
import { getTenantConfig, TenantMigrationStatus } from '../tenant'
import { readTenantMigrationVersion } from './connection'
import { highestLocalMigrationName, lastLocalMigrationName } from './files'
import { isDBMigrationName, isUnrecognizedMigration } from './guards'
import { DBMigration } from './types'

const { isMultitenant } = getConfig()

type ObservedTenant = Pick<
  Awaited<ReturnType<typeof getTenantConfig>>,
  | 'databaseUrl'
  | 'migrationVersion'
  | 'migrationStatus'
  | 'observedMigration'
  | 'observedMigrationName'
  | 'observedMigrationExpiresAt'
>

const failedReadRetryMs = 5_000
const aheadObservationTtlMs = 30_000

export function needsObservation(
  tenant: Pick<ObservedTenant, 'migrationVersion' | 'migrationStatus' | 'observedMigrationName'>
) {
  return (
    !isDBMigrationName(tenant.migrationVersion) ||
    tenant.migrationStatus !== TenantMigrationStatus.COMPLETED ||
    isUnrecognizedMigration(tenant.observedMigrationName)
  )
}

export function getCachedTenantMigration(tenant: ObservedTenant) {
  if (
    tenant.observedMigrationExpiresAt !== undefined &&
    performance.now() >= tenant.observedMigrationExpiresAt
  ) {
    tenant.observedMigration = undefined
    tenant.observedMigrationExpiresAt = undefined
    // Retain the ahead name until refresh so known control rows also recheck it.
  }
  return tenant.observedMigration
}

/** A completed row this binary does not know is ahead by design; its restores must refresh tenant metadata. */
function isCompletedAhead(tenant: Pick<ObservedTenant, 'migrationVersion' | 'migrationStatus'>) {
  return (
    isUnrecognizedMigration(tenant.migrationVersion) &&
    tenant.migrationStatus === TenantMigrationStatus.COMPLETED
  )
}

function recordObservation(tenant: ObservedTenant, applied: string) {
  tenant.observedMigrationName = applied
  tenant.observedMigrationExpiresAt =
    isUnrecognizedMigration(applied) && !isCompletedAhead(tenant)
      ? performance.now() + aheadObservationTtlMs
      : undefined
}

/** Cache an already observed run result using the same expiry as a ledger read. */
export function cacheTenantMigration(tenant: ObservedTenant, applied: string) {
  tenant.observedMigration = Promise.resolve(
    isDBMigrationName(applied) ? applied : highestLocalMigrationName()
  )
  recordObservation(tenant, applied)
}

/** Known heads last with config; ahead heads under a row that still needs repair, and failed reads, expire sooner. */
export function observeTenantMigration(tenantId: string, tenant: ObservedTenant) {
  const cached = getCachedTenantMigration(tenant)
  if (cached) {
    return cached
  }
  const { migrationVersion, migrationStatus } = tenant
  const observed: NonNullable<ObservedTenant['observedMigration']> = readTenantMigrationVersion({
    tenantId,
    databaseUrl: tenant.databaseUrl,
  })
    .then((applied) => {
      if (tenant.observedMigration === observed) recordObservation(tenant, applied)
      return isDBMigrationName(applied) ? applied : highestLocalMigrationName()
    })
    .catch((error) => {
      // Keep the outcome briefly so a failing direct connection is not retried per request.
      if (tenant.observedMigration === observed) {
        // Retain the name so observation and schema repair checks still apply after a failed refresh.
        tenant.observedMigrationExpiresAt = performance.now() + failedReadRetryMs
      }
      logSchema.warning(logger, '[Migrations] Ledger read failed', {
        type: 'migrations',
        error,
        project: tenantId,
      })
      if (isDBMigrationName(migrationVersion)) {
        return migrationVersion
      }
      if (isCompletedAhead({ migrationVersion, migrationStatus })) {
        // An older binary can serve a completed rollout through the request pool
        // even when the direct connection used for observation is unavailable.
        return highestLocalMigrationName()
      }
      throw StorageBackendError.withStatusCode(503, {
        code: ErrorCode.DatabaseError,
        httpStatusCode: 503,
        message: 'Unable to determine the database schema version. Please try again later.',
        originalError: error,
      })
    })
  tenant.observedMigration = observed
  return observed
}

export async function tenantHasMigrations(tenantId: string, migration: keyof typeof DBMigration) {
  let migrationVersion: keyof typeof DBMigration | undefined
  if (isMultitenant) {
    const tenant = await getTenantConfig(tenantId)
    migrationVersion = await (getCachedTenantMigration(tenant) ??
      (needsObservation(tenant)
        ? observeTenantMigration(tenantId, tenant)
        : tenant.migrationVersion))
  } else {
    migrationVersion = await lastLocalMigrationName()
  }

  return Boolean(migrationVersion && DBMigration[migrationVersion] >= DBMigration[migration])
}

/**
 * Determine if a tenant has the migrations up to date
 * @param tenantId
 */
export async function areMigrationsUpToDate(tenantId: string) {
  const latestMigrationVersion = await lastLocalMigrationName()
  const tenant = await getTenantConfig(tenantId)

  if (
    isDBMigrationName(tenant.observedMigrationName) &&
    DBMigration[tenant.observedMigrationName] < DBMigration[latestMigrationVersion]
  ) {
    return false
  }

  if (needsObservation(tenant)) {
    // An ahead head skips execution only while its ledger observation is fresh.
    const observed = await observeTenantMigration(tenantId, tenant).catch(() => undefined)
    if (isUnrecognizedMigration(tenant.observedMigrationName)) return true
    if (isUnrecognizedMigration(tenant.migrationVersion)) {
      return observed === highestLocalMigrationName()
    }
    if (observed && DBMigration[observed] < DBMigration[latestMigrationVersion]) return false
  }

  return (
    tenant.migrationVersion &&
    DBMigration[latestMigrationVersion] <= DBMigration[tenant.migrationVersion] &&
    tenant.migrationStatus === TenantMigrationStatus.COMPLETED
  )
}
