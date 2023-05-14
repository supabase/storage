import { getConfig } from '../config'
import { getTenantConfig } from './tenant'
import { StorageBackendError } from '../storage'
import { User, TenantConnection } from './connection'

interface ConnectionOptions {
  host: string
  tenantId: string
  headers?: Record<string, string | undefined | string[]>
  method?: string
  path?: string
  user: User
  superUser: User
}

/**
 * Creates a tenant specific knex client
 * @param options
 */
export async function getPostgresConnection(options: ConnectionOptions): Promise<TenantConnection> {
  const dbCredentials = await getDbCredentials(options.tenantId, options.host)

  return await TenantConnection.create({
    ...dbCredentials,
    ...options,
  })
}

async function getDbCredentials(tenantId: string, host: string | undefined) {
  const {
    isMultitenant,
    databasePoolURL,
    databaseURL,
    databaseMaxConnections,
    xForwardedHostRegExp,
  } = getConfig()

  let dbUrl = databasePoolURL || databaseURL
  let maxConnections = databaseMaxConnections
  let isExternalPool = Boolean(databasePoolURL)

  if (isMultitenant && xForwardedHostRegExp) {
    if (!tenantId) {
      throw new StorageBackendError('Invalid Tenant Id', 400, 'Tenant id not provided')
    }

    const xForwardedHost = host

    if (typeof xForwardedHost !== 'string') {
      throw new StorageBackendError(
        'Invalid Header',
        400,
        'X-Forwarded-Host header is not a string'
      )
    }
    if (!new RegExp(xForwardedHostRegExp).test(xForwardedHost)) {
      throw new StorageBackendError(
        'Invalid Header',
        400,
        'X-Forwarded-Host header does not match regular expression'
      )
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
