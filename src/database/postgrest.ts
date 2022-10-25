import { PostgrestClient } from '@supabase/postgrest-js'
import { getConfig } from '../config'
import { getAnonKey } from './tenant'
import { StorageBackendError } from '../storage'

interface PostgrestClientOptions {
  host?: string
  tenantId?: string
}

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
    },
    schema: 'storage',
  })
}
