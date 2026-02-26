import { Cluster } from '@internal/cluster'
import { ERRORS } from '@internal/errors'
import { getConfig } from '../../config'
import { TenantConnection } from './connection'
import { User } from './pool'
import { getTenantConfig } from './tenant'

interface ConnectionOptions {
  host: string
  tenantId: string
  maxConnections?: number
  headers?: Record<string, string | undefined | string[]>
  method?: string
  path?: string
  user: User
  superUser: User
  disableHostCheck?: boolean
  operation?: () => string | undefined
}

/**
 * Creates a tenant specific knex client
 * @param options
 */
export async function getPostgresConnection(options: ConnectionOptions): Promise<TenantConnection> {
  const dbCredentials = await getDbSettings(options.tenantId, options.host, {
    disableHostCheck: options.disableHostCheck,
  })

  return await TenantConnection.create({
    ...dbCredentials,
    ...options,
    clusterSize: Cluster.size,
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
    databasePoolMode,
  } = getConfig()

  let dbUrl = databasePoolURL || databaseURL
  let maxConnections = databaseMaxConnections
  let isExternalPool = Boolean(databasePoolURL)
  let isSingleUse = !databasePoolMode || databasePoolMode === 'single_use'

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
    isSingleUse = tenant.databasePoolMode ? tenant.databasePoolMode !== 'recycled' : isSingleUse
  }

  return {
    dbUrl,
    isExternalPool,
    maxConnections,
    isSingleUse,
  }
}
