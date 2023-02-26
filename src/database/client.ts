import { PostgrestClient } from '@supabase/postgrest-js'
import { getConfig } from '../config'
import { getAnonKey, getTenantConfig } from './tenant'
import { StorageBackendError } from '../storage'
import { verifyJWT } from '../auth'
import { connections, TenantConnection } from './connection'

interface PostgrestClientOptions {
  host?: string
  tenantId?: string
  forwardHeaders?: Record<string, string>
}

/**
 * Creates a tenant specific postgrest client
 * @param jwt
 * @param options
 */
export async function getPostgrestClient(
  jwt: string,
  options: PostgrestClientOptions
): Promise<PostgrestClient> {
  const {
    anonKey,
    isMultitenant,
    postgrestURL,
    postgrestURLScheme,
    postgrestURLSuffix,
    xForwardedHostRegExp,
  } = getConfig()

  let url = postgrestURL
  let apiKey = anonKey

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

    url = `${postgrestURLScheme}://${xForwardedHost}${postgrestURLSuffix}`
    apiKey = await getAnonKey(options.tenantId)
  }

  return new PostgrestClient(url, {
    headers: {
      apiKey,
      Authorization: `Bearer ${jwt}`,
      ...(options.forwardHeaders || {}),
    },
    schema: 'storage',
  })
}

interface ConnectionOptions {
  host?: string
  tenantId?: string
  forwardHeaders?: Record<string, string>
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
  const { jwtSecret, isMultitenant, databaseUrl, xForwardedHostRegExp } = getConfig()

  let url = databaseUrl
  let jwtSecretKey = jwtSecret

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
    url = tenant.databaseUrl
    jwtSecretKey = tenant.jwtSecret
  }

  const verifiedJWT = await verifyJWT(jwt, jwtSecretKey)

  if (!verifiedJWT) {
    throw new StorageBackendError('invalid_jwt', 403, 'invalid jwt')
  }

  const role = verifiedJWT?.role || 'anon'

  return await TenantConnection.create(connections.values, {
    url: url as string,
    role,
    jwt: verifiedJWT,
    jwtRaw: jwt,
    jwtSecret,
  })
}
