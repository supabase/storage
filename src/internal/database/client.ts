import { getConfig } from '../../config'
import { getTenantConfig } from './tenant'
import { User, TenantConnection } from './connection'
import { ERRORS } from '@internal/errors'

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

/**
 * Creates a tenant specific knex client
 * @param options
 */
export async function getPostgresConnection(options: ConnectionOptions): Promise<TenantConnection> {
  const dbCredentials = await getDbCredentials(options.tenantId, options.host, {
    disableHostCheck: options.disableHostCheck,
  })

  return await TenantConnection.create({
    ...dbCredentials,
    ...options,
  })
}

async function getDbCredentials(
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
