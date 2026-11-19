import { setTimeout as delay } from 'node:timers/promises'
import { purgeCdnCache } from '../support/cdn-purge'
import { describeAcceptance, encodePathSegments, getAcceptanceConfig } from '../support/config'
import { AcceptanceHttpClient, createRestClient } from '../support/http'
import {
  createRestBucket,
  requireServiceKey,
  uniqueBucketName,
  uniqueObjectKey,
  uploadRestObject,
} from '../support/resources'

type AccessMethod = 'public' | 'authenticated' | 'signed'

interface TransformParams {
  width: number
  height: number
}

interface VersionedObjectUrlOptions {
  transform?: TransformParams
  versionId?: string
  expiresIn?: number
}

interface SignedUrlResponse {
  signedURL: string
}

const onePixelPng = new Uint8Array(
  Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p94AAAAASUVORK5CYII=',
    'base64'
  )
)

const updatedPixelPng = new Uint8Array(
  Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGP4/x8AAwAB//wl3FEAAAAASUVORK5CYII=',
    'base64'
  )
)

const SIGNED_EXPIRES_IN_S = 300
const CACHE_RETRIES = 15
// Adapted from cdn.test.ts's KV_SETTLE_DELAY_MS - see the file-level comment above.
const KV_SETTLE_DELAY_MS = 5000

async function pauseForWebhookIfNeeded(accessMethod: AccessMethod) {
  if (accessMethod === 'signed' && getAcceptanceConfig().cdnVersion !== '2') {
    await delay(KV_SETTLE_DELAY_MS)
  }
}

interface ObjectInfoResponse {
  id: string | null
  name: string
  version: string
  bucket_id: string
  size: number | null
  content_type: string | null
  cache_control: string | null
  etag: string | null
  metadata: Record<string, unknown> | null
  last_modified: string | null
  created_at: string | null
  archived_at: string | null
  is_delete_marker: boolean
  is_versioned: boolean
}

interface ObjectVersionRow {
  name: string
  version: string
  archived_at: string | null
  is_delete_marker: boolean
  is_versioned: boolean
}

interface ListObjectsV2Response {
  folders: Array<{ name: string }>
  hasNext?: boolean
  nextCursor?: string
  objects: ObjectVersionRow[]
}

interface ErrorResponse {
  error?: string
  message?: string
  statusCode?: string
}

async function getInfo(
  client: AcceptanceHttpClient,
  token: string,
  bucketName: string,
  key: string,
  options: { versionId?: string; expectedStatus?: number | number[] } = {}
) {
  const qs = options.versionId ? `?versionId=${encodeURIComponent(options.versionId)}` : ''
  return client.request<ObjectInfoResponse>(
    'GET',
    `/object/info/authenticated/${bucketName}/${encodePathSegments(key)}${qs}`,
    { expectedStatus: options.expectedStatus ?? 200, token }
  )
}

async function bulkDelete(
  client: AcceptanceHttpClient,
  token: string,
  bucketName: string,
  prefixes: Array<string | { path: string; versionId: string }>,
  expectedStatus: number | number[] = 200
) {
  return client.request<ObjectVersionRow[]>('DELETE', `/object/${bucketName}`, {
    body: { prefixes },
    expectedStatus,
    token,
  })
}

async function listAllVersions(
  client: AcceptanceHttpClient,
  token: string,
  bucketName: string
): Promise<ObjectVersionRow[]> {
  const rows: ObjectVersionRow[] = []
  let cursor: string | undefined

  do {
    const page = await client.request<ListObjectsV2Response>(
      'POST',
      `/object/list-v2/${bucketName}`,
      {
        body: {
          cursor,
          deleteMarkers: 'include',
          limit: 1000,
          noncurrentVersions: 'include',
          prefix: '',
          with_delimiter: false,
        },
        expectedStatus: 200,
        token,
      }
    )
    rows.push(...(page.json?.objects ?? []))
    cursor = page.json?.hasNext ? page.json?.nextCursor : undefined
  } while (cursor)

  return rows
}

async function purgeVersionedBucket(
  client: AcceptanceHttpClient,
  token: string,
  bucketName: string
) {
  const warn = (step: string, error: unknown) =>
    console.warn(
      `[versioning cleanup] ${step} failed for bucket ${bucketName}; it may be left behind: ${
        error instanceof Error ? error.message : String(error)
      }`
    )

  // Already gone (or never created): nothing to purge, and nothing to warn about. A
  // pre-check that fails outright says nothing either way, so fall through and purge.
  const existing = await client
    .request('GET', `/bucket/${bucketName}`, { expectedStatus: [200, 400, 404], token })
    .catch((error) => {
      warn('checking whether the bucket still exists', error)
      return undefined
    })
  if (existing && existing.status !== 200) {
    return
  }

  const rows = await listAllVersions(client, token, bucketName).catch((error) => {
    warn('enumerating versions', error)
    return []
  })
  const prefixes = rows.map((row) => ({ path: row.name, versionId: row.version }))

  if (prefixes.length > 0) {
    await bulkDelete(client, token, bucketName, prefixes).catch((error) =>
      warn('hard-deleting versions', error)
    )
  }

  const deleted = await client
    .request<ErrorResponse>('DELETE', `/bucket/${bucketName}`, {
      expectedStatus: [200, 400, 404],
      token,
    })
    .catch((error) => {
      warn('deleting the bucket', error)
      return undefined
    })

  // A 400 here is the API's "not found" shape when the bucket is already gone;
  // anything else (e.g. ResourceNotEmpty) means rows survived the purge.
  if (deleted && deleted.status !== 200 && deleted.json?.statusCode !== '404') {
    warn('deleting the bucket', deleted.body)
  }
}

async function getVersionedObjectUrl(
  bucketName: string,
  objectKey: string,
  accessMethod: AccessMethod,
  { transform, versionId, expiresIn = SIGNED_EXPIRES_IN_S }: VersionedObjectUrlOptions = {}
): Promise<{ route: string; token?: string }> {
  const config = getAcceptanceConfig()
  const encodedKey = encodePathSegments(objectKey)
  const routePrefix = transform ? 'render/image' : 'object'
  const queryParams = new URLSearchParams()
  if (transform) {
    queryParams.set('width', String(transform.width))
    queryParams.set('height', String(transform.height))
  }
  if (versionId) {
    queryParams.set('versionId', versionId)
  }
  const query = [...queryParams].length > 0 ? `?${queryParams.toString()}` : ''

  if (accessMethod === 'public') {
    return { route: `/${routePrefix}/public/${bucketName}/${encodedKey}${query}` }
  }

  if (accessMethod === 'authenticated') {
    return {
      token: requireServiceKey(config),
      route: `/${routePrefix}/authenticated/${bucketName}/${encodedKey}${query}`,
    }
  }

  const client = createRestClient()
  const signed = await client.request<SignedUrlResponse>(
    'POST',
    `/object/sign/${bucketName}/${encodedKey}`,
    {
      body: {
        expiresIn,
        versionId,
        ...(transform
          ? { transform: { height: transform.height, width: transform.width, resize: 'contain' } }
          : {}),
      },
      expectedStatus: 200,
      token: requireServiceKey(config),
    }
  )
  return { route: signed.json?.signedURL ?? '' }
}

async function warmCacheEndpoint(client: AcceptanceHttpClient, route: string, token?: string) {
  const miss = await client.request('GET', route, {
    expectedCacheStatus: 'MISS',
    expectedStatus: 200,
    token,
    retries: CACHE_RETRIES,
  })
  const hit = await client.request('GET', route, {
    expectedCacheStatus: 'HIT',
    expectedStatus: 200,
    token,
    retries: CACHE_RETRIES,
  })
  return { hit, miss }
}

;(['public', 'authenticated', 'signed'] as const).forEach((accessMethod) => {
  const isPublic = accessMethod === 'public'
  const isSigned = accessMethod === 'signed'

  describeAcceptance(
    `CDN object versioning - ${accessMethod} access`,
    {
      destructive: true,
      profiles: ['full'],
      requires: ['cdnEdge', 'versioning'],
    },
    () => {
      const canRender = getAcceptanceConfig().capabilities.render
      const isCdnV2 = getAcceptanceConfig().cdnVersion === '2'
      let client: AcceptanceHttpClient
      let bucketName: string

      beforeAll(async () => {
        client = createRestClient()
        bucketName = uniqueBucketName(`cdn-ver-${accessMethod}`)
        await createRestBucket(bucketName, { isPublic, versioningStatus: 'ENABLED' })
      })

      afterAll(async () => {
        await purgeVersionedBucket(client, requireServiceKey(), bucketName)
      })

      async function uploadAndGetVersion(
        objectKey: string,
        content: BodyInit,
        contentType?: string
      ): Promise<{ version: string; etag: string }> {
        await uploadRestObject(bucketName, objectKey, content, { contentType })
        const info = await getInfo(client, requireServiceKey(), bucketName, objectKey)
        return {
          version: info.json?.version as string,
          etag: info.json?.etag as string,
        }
      }

      it('serves distinct, independently-cached content for each explicit versionId', async () => {
        const objectKey = uniqueObjectKey('ver-explicit')
        const a = await uploadAndGetVersion(objectKey, 'content-a')
        const b = await uploadAndGetVersion(objectKey, 'content-b')
        expect(a.etag).toBeTruthy()
        expect(b.etag).toBeTruthy()
        expect(a.etag).not.toBe(b.etag)

        await pauseForWebhookIfNeeded(accessMethod)

        const { route: routeA, token: tokenA } = await getVersionedObjectUrl(
          bucketName,
          objectKey,
          accessMethod,
          { versionId: a.version }
        )
        const { route: routeB, token: tokenB } = await getVersionedObjectUrl(
          bucketName,
          objectKey,
          accessMethod,
          { versionId: b.version }
        )
        const { route: routeCurrent, token: tokenCurrent } = await getVersionedObjectUrl(
          bucketName,
          objectKey,
          accessMethod
        )

        const { hit: hitA, miss: missA } = await warmCacheEndpoint(client, routeA, tokenA)
        expect(missA.body).toBe('content-a')
        expect(missA.headers.get('etag')).toBe(a.etag)
        expect(hitA.body).toBe('content-a')
        expect(hitA.headers.get('etag')).toBe(a.etag)

        // B is a fresh MISS despite A already being warmed under the same object key
        // proves the versionId segment of the cache key actually discriminates between versions
        const { hit: hitB, miss: missB } = await warmCacheEndpoint(client, routeB, tokenB)
        expect(missB.body).toBe('content-b')
        expect(missB.headers.get('etag')).toBe(b.etag)
        expect(hitB.body).toBe('content-b')
        expect(hitB.headers.get('etag')).toBe(b.etag)

        // "current" (no versionId) resolves to B.
        const current = await client.request('GET', routeCurrent, {
          // For public/authenticated, the object-bytes cache key is etag-only (no query string)
          // so the cache is pre-warmed and a HIT is expected.
          // Signed includes the token in the cache key currently so it is expected to MISS
          expectedCacheStatus: isSigned ? 'MISS' : 'HIT',
          expectedStatus: 200,
          retries: CACHE_RETRIES,
          token: tokenCurrent,
        })
        expect(current.body).toBe('content-b')
        expect(current.headers.get('etag')).toBe(b.etag)
      })

      it.skipIf(!canRender)(
        'serves distinct, independently-cached content for each explicit versionId on a render/transform route',
        async () => {
          const objectKey = uniqueObjectKey('ver-explicit-transform', 'png')
          const a = await uploadAndGetVersion(objectKey, onePixelPng, 'image/png')
          const b = await uploadAndGetVersion(objectKey, updatedPixelPng, 'image/png')
          expect(a.etag).not.toBe(b.etag)

          await delay(KV_SETTLE_DELAY_MS)

          const { route: routeA, token: tokenA } = await getVersionedObjectUrl(
            bucketName,
            objectKey,
            accessMethod,
            { versionId: a.version, transform: { width: 10, height: 10 } }
          )
          const { route: routeB, token: tokenB } = await getVersionedObjectUrl(
            bucketName,
            objectKey,
            accessMethod,
            { versionId: b.version, transform: { width: 10, height: 10 } }
          )

          const { hit: hitA } = await warmCacheEndpoint(client, routeA, tokenA)
          const etagA = hitA.headers.get('etag')
          expect(etagA).toBeTruthy()

          const { hit: hitB, miss: missB } = await warmCacheEndpoint(client, routeB, tokenB)
          expect(missB.status).toBe(200)
          const etagB = hitB.headers.get('etag')
          expect(etagB).toBeTruthy()

          // Different source versions must render to genuinely different bytes
          expect(etagB).not.toBe(etagA)
        }
      )

      it(
        'moving the current version does not disturb an explicit historical versionId, ' +
          'and the current route still invalidates normally',
        async () => {
          const objectKey = uniqueObjectKey('ver-current-move')
          const a = await uploadAndGetVersion(objectKey, 'content-a')

          await pauseForWebhookIfNeeded(accessMethod)

          const { route: currentRoute, token: currentToken } = await getVersionedObjectUrl(
            bucketName,
            objectKey,
            accessMethod
          )
          const { miss: missA, hit: hitA } = await warmCacheEndpoint(
            client,
            currentRoute,
            currentToken
          )
          expect(missA.body).toBe('content-a')
          expect(missA.headers.get('etag')).toBe(a.etag)
          expect(hitA.body).toBe('content-a')
          expect(hitA.headers.get('etag')).toBe(a.etag)

          const b = await uploadAndGetVersion(objectKey, 'content-b')
          expect(b.etag).not.toBe(a.etag)
          if (isCdnV2) {
            // TODO(lenny): remove once storage active purge is deployed
            await purgeCdnCache(client, { bucketName, objectKey })
          }
          await pauseForWebhookIfNeeded(accessMethod)

          const { route: afterRoute, token: afterToken } = await getVersionedObjectUrl(
            bucketName,
            objectKey,
            accessMethod
          )
          const afterUpdate = await client.request('GET', afterRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
            token: afterToken,
          })
          expect(afterUpdate.body).toBe('content-b')
          expect(afterUpdate.headers.get('etag')).toBe(b.etag)

          // Explicit historical lookup shares the object-bytes entry the
          // very first "current" warm-up above already cached for etag A
          // except for signed, whose cache key is the token, not the etag.
          const { route: historicalRoute, token: historicalToken } = await getVersionedObjectUrl(
            bucketName,
            objectKey,
            accessMethod,
            { versionId: a.version }
          )
          const historical = await client.request('GET', historicalRoute, {
            expectedCacheStatus: isSigned ? 'MISS' : 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
            token: historicalToken,
          })
          expect(historical.body).toBe('content-a')
          expect(historical.headers.get('etag')).toBe(a.etag)
        }
      )

      it.skipIf(!canRender)(
        'moving the current version does not disturb an explicit historical versionId on a render/transform route',
        async () => {
          const objectKey = uniqueObjectKey('ver-current-move-transform', 'png')
          const a = await uploadAndGetVersion(objectKey, onePixelPng, 'image/png')

          await delay(KV_SETTLE_DELAY_MS)

          const { route: currentRoute, token: currentToken } = await getVersionedObjectUrl(
            bucketName,
            objectKey,
            accessMethod,
            { transform: { width: 10, height: 10 } }
          )
          const { hit: hitA } = await warmCacheEndpoint(client, currentRoute, currentToken)
          const etagA = hitA.headers.get('etag')
          expect(etagA).toBeTruthy()

          const b = await uploadAndGetVersion(objectKey, updatedPixelPng, 'image/png')
          expect(b.etag).not.toBe(a.etag)
          if (isCdnV2) {
            // TODO(lenny): remove once storage active purge is deployed
            await purgeCdnCache(client, { bucketName, objectKey })
          } else {
            // PNG responses are aggressively cached for 60s on V1
            // V2 doesn't need this since it purges by tag above instead.
            await delay(60_000)
          }

          const { route: afterRoute, token: afterToken } = await getVersionedObjectUrl(
            bucketName,
            objectKey,
            accessMethod,
            { transform: { width: 10, height: 10 } }
          )
          const afterUpdate = await client.request('GET', afterRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
            token: afterToken,
          })
          const etagAfter = afterUpdate.headers.get('etag')
          expect(etagAfter).toBeTruthy()
          expect(etagAfter).not.toBe(etagA)

          const { route: historicalRoute, token: historicalToken } = await getVersionedObjectUrl(
            bucketName,
            objectKey,
            accessMethod,
            { versionId: a.version, transform: { width: 10, height: 10 } }
          )
          const historical = await client.request('GET', historicalRoute, {
            expectedCacheStatus: isSigned ? 'MISS' : 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
            token: historicalToken,
          })
          expect(historical.headers.get('etag')).toBe(etagA)
        }
      )
    },
    90_000
  )
})
