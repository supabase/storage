import { Storage } from '@storage/storage'
import axios, { AxiosError } from 'axios'
import { HttpsAgent } from 'agentkeepalive'
import { ERRORS } from '@internal/errors'

import { getConfig } from '../../config'

const { cdnPurgeEndpointURL, cdnPurgeEndpointKey } = getConfig()

const httpsAgent = new HttpsAgent({
  keepAlive: true,
  maxFreeSockets: 20,
  maxSockets: 200,
  freeSocketTimeout: 1000 * 2,
})

const client = axios.create({
  baseURL: cdnPurgeEndpointURL,
  httpsAgent: httpsAgent,
  headers: {
    Authorization: `Bearer ${cdnPurgeEndpointKey}`,
    'Content-Type': 'application/json',
  },
})

export interface PurgeCacheInput {
  tenant: string
  bucket: string
  objectName: string
}

export class CdnCacheManager {
  constructor(protected readonly storage: Storage) {}

  async purge(opts: PurgeCacheInput) {
    if (!cdnPurgeEndpointURL) {
      throw ERRORS.MissingParameter('CDN_PURGE_ENDPOINT_URL is not set')
    }

    // Check if object exists
    await this.storage.from(opts.bucket).asSuperUser().findObject({ objectName: opts.objectName })

    // Purge cache
    try {
      await client.post('/purge', {
        tenant: {
          ref: opts.tenant,
        },
        bucketId: opts.bucket,
        objectName: opts.objectName,
      })
    } catch (e) {
      if (e instanceof AxiosError) {
        throw ERRORS.InternalError(e, 'Error purging cache')
      }

      throw e
    }
  }
}
