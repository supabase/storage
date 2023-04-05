import { getConfig } from '../config'
import { getTenantConfig } from './tenant'
import { StorageBackendError } from '../storage'
import { verifyJWT } from '../auth'
import { TenantConnection } from './connection'

interface ConnectionOptions {
  host?: string
  tenantId?: string
  forwardHeaders?: Record<string, string | undefined | string[]>
  method?: string
  path?: string
}

/**
 * Creates a tenant specific knex client
 * @param jwt
 * @param options
 */
export async function getPostgresConnection(
  jwt: string,
  options: ConnectionOptions
): Promise<TenantConnection> {
  const {
    jwtSecret,
    isMultitenant,
    databasePoolURL,
    databaseURL,
    databaseMaxConnections,
    xForwardedHostRegExp,
  } = getConfig()

  let dbUrl = databasePoolURL || databaseURL
  let jwtSecretKey = jwtSecret
  let maxConnections = databaseMaxConnections
  let isExternalPool = Boolean(databasePoolURL)

  if (isMultitenant && xForwardedHostRegExp) {
    const xForwardedHost = options.host
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

    if (!options.tenantId) {
      throw new StorageBackendError('Invalid Tenant Id', 400, 'Tenant id not provided')
    }

    const tenant = await getTenantConfig(options.tenantId)
    dbUrl = tenant.databasePoolUrl || tenant.databaseUrl
    isExternalPool = Boolean(tenant.databasePoolUrl)
    maxConnections = tenant.maxConnections ?? maxConnections
    jwtSecretKey = tenant.jwtSecret
  }

  const verifiedJWT = await verifyJWT(jwt, jwtSecretKey)

  if (!verifiedJWT) {
    throw new StorageBackendError('invalid_jwt', 403, 'invalid jwt')
  }

  const role = verifiedJWT?.role || 'anon'

  return await TenantConnection.create({
    tenantId: options.tenantId as string,
    dbUrl,
    isExternalPool,
    maxConnections,
    role,
    jwt: verifiedJWT,
    jwtRaw: jwt,
    headers: options.forwardHeaders,
    method: options.method,
    path: options.path,
  })
}
