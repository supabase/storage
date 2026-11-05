import { setTimeout as delay } from 'node:timers/promises'
import { encodePathSegments } from './config'
import { AcceptanceHttpClient } from './http'
import { requireServiceKey } from './resources'

interface PurgeOptions {
  bucketName?: string
  objectKey?: string
  transformations?: boolean
}

interface CdnPurgeResponse {
  message: string
  statusCode?: string
}

export async function purgeCdnCache(
  client: AcceptanceHttpClient,
  { bucketName, objectKey, transformations }: PurgeOptions = {}
) {
  let url = '/cdn'
  if (bucketName) {
    url += '/' + bucketName
  }
  if (objectKey) {
    url += '/' + encodePathSegments(objectKey)
  }
  if (transformations) {
    url += '?transformations=true'
  }

  const result = await client.request<CdnPurgeResponse>('DELETE', url, {
    expectedStatus: 200,
    token: requireServiceKey(),
    retries: 3,
  })
  await delay(2000)
  return result
}
