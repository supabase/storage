import { FastifyRequest } from 'fastify'
import { PostgrestClient } from '@supabase/postgrest-js'
import { getConfig } from '../config'
import { getAnonKey } from './tenant'
import { StorageBackendError } from '../storage'

/**
 * Creates a tenant specific postgrest client
 * @param request
 * @param jwt
 */
export async function getPostgrestClient(
  request: FastifyRequest,
  jwt: string
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
    const xForwardedHost = request.headers['x-forwarded-host']
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
    url = `${postgrestURLScheme}://${xForwardedHost}${postgrestURLSuffix}`
    apiKey = await getAnonKey(request.tenantId)
  }

  return new PostgrestClient(url, {
    headers: {
      apiKey,
      Authorization: `Bearer ${jwt}`,
    },
    schema: 'storage',
  })
}
