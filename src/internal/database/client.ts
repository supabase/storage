import { Cluster } from '@internal/cluster'
import { ERRORS } from '@internal/errors'
import { hasField } from '@platformatic/globals'
import { getConfig } from '../../config'
import { PgTenantConnection } from './pg-connection'
import { User } from './pool'
import { getTenantConfig } from './tenant'
import { getWattPostgresConnection } from './watt-connection'

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
  const dbCredentials = await getDbSettings(options.tenantId, options.host, {
    disableHostCheck: options.disableHostCheck,
  })

  return await PgTenantConnection.create({
    ...dbCredentials,
    ...options,
    clusterSize: Cluster.size,
  })
}

function validateConnectionOptions(options: ConnectionOptions): void {
  const { isMultitenant, requestXForwardedHostRegExp } = getConfig()

  if (!isMultitenant) {
    return
  }

  if (!options.tenantId) {
    throw ERRORS.InvalidTenantId()
  }

  if (!requestXForwardedHostRegExp || options.disableHostCheck) {
    return
  }

  const xForwardedHost = options.host

  if (typeof xForwardedHost !== 'string') {
    throw ERRORS.InvalidXForwardedHeader('X-Forwarded-Host header is not a string')
  }

  if (!new RegExp(requestXForwardedHostRegExp).test(xForwardedHost)) {
    throw ERRORS.InvalidXForwardedHeader(
      'X-Forwarded-Host header does not match regular expression'
    )
  }
}

export async function getPostgresConnection(
  options: ConnectionOptions
): Promise<PgTenantConnection> {
  if (!hasField('messaging')) {
    return getPgPostgresConnection(options)
  }

  validateConnectionOptions(options)
  const { databaseMaxConnections } = getConfig()

  return getWattPostgresConnection({
    ...options,
    dbUrl: '',
    isExternalPool: false,
    isSingleUse: false,
    maxConnections: options.maxConnections ?? databaseMaxConnections,
  })
}



async function getDbSettings(
  tenantId: string,
  host: string | undefined,
  options?: { disableHostCheck?: boolean }
) {
  const {
    isMultitenant,
    databasePoolURL,
    databaseURL,
    databaseMaxConnections,
    requestXForwardedHostRegExp,
  } = getConfig()

  let dbUrl = databasePoolURL || databaseURL
  let maxConnections = databaseMaxConnections
  let isExternalPool = Boolean(databasePoolURL)

  if (isMultitenant) {
    if (!tenantId) {
      throw ERRORS.InvalidTenantId()
    }

    if (requestXForwardedHostRegExp && !options?.disableHostCheck) {
      const xForwardedHost = host

      if (typeof xForwardedHost !== 'string') {
        throw ERRORS.InvalidXForwardedHeader('X-Forwarded-Host header is not a string')
      }
      if (!new RegExp(requestXForwardedHostRegExp).test(xForwardedHost)) {
        throw ERRORS.InvalidXForwardedHeader(
          'X-Forwarded-Host header does not match regular expression'
        )
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
