import { ERRORS } from '@internal/errors'
import { Storage } from '@storage/storage'
import { Agent } from 'undici'

import { getConfig } from '../../config'

const { cdnPurgeEndpointURL, cdnPurgeEndpointKey } = getConfig()

const CDN_PURGE_TIMEOUT_MS = 10_000

const dispatcher = new Agent({
  connections: 200,
  keepAliveTimeout: 1000 * 2,
  keepAliveMaxTimeout: 1000 * 2,
})

const defaultHeaders = new Headers({
  'Content-Type': 'application/json',
  ...(cdnPurgeEndpointKey ? { Authorization: `Bearer ${cdnPurgeEndpointKey}` } : {}),
})

const cdnPurgeUrl = cdnPurgeEndpointURL ? resolvePurgeUrl(cdnPurgeEndpointURL) : undefined

export interface PurgeCacheInput {
  tenant: string
  bucket: string
  objectName: string
}

function resolvePurgeUrl(baseURL: string) {
  const url = new URL(baseURL)
  url.pathname = url.pathname.endsWith('/') ? `${url.pathname}purge` : `${url.pathname}/purge`
  url.search = ''
  url.hash = ''

  return url.toString()
}

async function assertOkResponse(response: Response) {
  if (response.ok) {
    return
  }

  throw new Error(`Request failed with status code ${response.status}`)
}

export class CdnCacheManager {
  constructor(protected readonly storage: Storage) {}

  async purge(opts: PurgeCacheInput) {
    if (!cdnPurgeUrl) {
      throw ERRORS.MissingParameter('CDN_PURGE_ENDPOINT_URL is not set')
    }

    // Check if object exists
    await this.storage.from(opts.bucket).asSuperUser().findObject(opts.objectName)

    // Purge cache
    try {
      const requestInit: RequestInit & { dispatcher: Agent } = {
        method: 'POST',
        headers: defaultHeaders,
        body: JSON.stringify({
          tenant: {
            ref: opts.tenant,
          },
          bucketId: opts.bucket,
          objectName: opts.objectName,
        }),
        dispatcher,
        signal: AbortSignal.timeout(CDN_PURGE_TIMEOUT_MS),
      }

      const response = await fetch(cdnPurgeUrl, requestInit)

      try {
        await assertOkResponse(response)
      } finally {
        await response.body?.cancel().catch(() => {})
      }
    } catch (e) {
      throw ERRORS.InternalError(
        e instanceof Error ? e : new Error(String(e)),
        'Error purging cache'
      )
    }
  }
}
