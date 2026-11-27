import { Cluster } from '@internal/cluster'
import { ERRORS } from '@internal/errors'
import { getXForwardedHostRegExp } from '@internal/http/x-forwarded-host'
import { logger, logSchema } from '@internal/monitoring'
import { hasField } from '@platformatic/globals'
import { getConfig } from '../../config'
import type { TenantConnection } from './connection'
import { PgTenantConnection } from './pg-connection'
import type { User } from './pool'
import { getTenantConfig } from './tenant'
import { getWattPostgresConnection } from './watt/connection'

const xForwardedHostRegExp = getXForwardedHostRegExp()

interface ConnectionOptions {
  host: string
  tenantId: string
  headers?: Record<string, string | undefined | string[]>
  method?: string
  path?: string
  user: User
  superUser: User
  disableHostCheck?: boolean
  operation?: () => string | undefined
}

export async function getPgPostgresConnection(
  options: ConnectionOptions
): Promise<PgTenantConnection> {
  return PgTenantConnection.create(await resolveConnectionOptions(options))
}

export async function getPostgresConnection(options: ConnectionOptions): Promise<TenantConnection> {
  const connectionOptions = await resolveConnectionOptions(options)

  if (!usesDatabaseWatt()) {
    return PgTenantConnection.create(connectionOptions)
  }

  return getWattPostgresConnection(connectionOptions)
}

let databaseWattRouting: boolean | undefined

// Decided once per worker. DATABASE_WATT_APPLICATION_ENABLED only takes effect when the
// runtime runs the Database application too, which watt-db.json declares by setting
// DATABASE_WATT_APPLICATION_PRESENT for the storage application: under watt.json every
// query would otherwise be sent to an application that does not exist.
function usesDatabaseWatt(): boolean {
  if (databaseWattRouting !== undefined) {
    return databaseWattRouting
  }

  const { databaseWattApplicationEnabled, databaseWattApplicationPresent } = getConfig()
  databaseWattRouting =
    databaseWattApplicationEnabled && databaseWattApplicationPresent && hasField('messaging')

  if (databaseWattApplicationEnabled && !databaseWattRouting) {
    logSchema.error(
      logger,
      '[DatabaseWatt] DATABASE_WATT_APPLICATION_ENABLED is set but this runtime does not run the Database application (start it with watt-db.json): using direct PostgreSQL connections',
      { type: 'db' }
    )
  }

  return databaseWattRouting
}

async function resolveConnectionOptions(options: ConnectionOptions) {
  const dbCredentials = await getDbSettings(options.tenantId, options.host, {
    disableHostCheck: options.disableHostCheck,
  })

  return {
    ...dbCredentials,
    ...options,
    clusterSize: Cluster.size,
  }
}

async function getDbSettings(
  tenantId: string,
  host: string | undefined,
  options?: { disableHostCheck?: boolean }
) {
  const { isMultitenant, databasePoolURL, databaseURL, databaseMaxConnections } = getConfig()

  let dbUrl = databasePoolURL || databaseURL
  let maxConnections = databaseMaxConnections
  let isExternalPool = Boolean(databasePoolURL)

  if (isMultitenant) {
    if (!tenantId) {
      throw ERRORS.InvalidTenantId()
    }

    if (!options?.disableHostCheck) {
      if (xForwardedHostRegExp) {
        const xForwardedHost = host

        if (typeof xForwardedHost !== 'string') {
          throw ERRORS.InvalidXForwardedHeader('X-Forwarded-Host header is not a string')
        }
        if (!xForwardedHostRegExp.test(xForwardedHost)) {
          throw ERRORS.InvalidXForwardedHeader(
            'X-Forwarded-Host header does not match regular expression'
          )
        }
      }
    }

    const tenant = await getTenantConfig(tenantId)
    dbUrl = tenant.databasePoolUrl || tenant.databaseUrl
    isExternalPool = Boolean(tenant.databasePoolUrl)
    maxConnections = tenant.maxConnections ?? maxConnections
  }

  return {
    dbUrl,
    isExternalPool,
    maxConnections,
  }
}
