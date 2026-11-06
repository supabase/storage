import { setTimeout as delay } from 'node:timers/promises'
import { encodePathSegments } from './config'
import { AcceptanceHttpClient } from './http'
import { requireServiceKey } from './resources'

/**
 * TEMPORARY: storage does not yet purge the CDN cache by tag itself on object mutation
 */
export async function purgeObjectCacheForMutation(
  client: AcceptanceHttpClient,
  bucketName: string,
  objectKey: string
) {
  const encodedKey = encodePathSegments(objectKey)

  await client.request('DELETE', `/cdn/${bucketName}/${encodedKey}`, {
    expectedStatus: 200,
    token: requireServiceKey(),
  })

  await delay(5000)
}
