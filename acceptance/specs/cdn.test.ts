import { setTimeout as delay } from 'node:timers/promises'
import {
  describeAcceptance,
  encodePathSegments,
  getAcceptanceConfig,
  requireConfigValue,
} from '../support/config'
import { AcceptanceHttpClient, createRestClient, parseStorageError } from '../support/http'
import {
  cleanupRestObjects,
  cleanupRestResources,
  createRestBucket,
  requireServiceKey,
  uniqueBucketName,
  uniqueObjectKey,
  uploadRestObject,
} from '../support/resources'

interface CdnPurgeResponse {
  message: string
  statusCode?: string
}

interface SignedUrlResponse {
  signedURL: string
}

type BucketType = 'public' | 'private'
type AccessMethod = 'public' | 'authenticated' | 'signed'
type CacheStatus = 'HIT' | 'MISS' | 'REVALIDATED' | 'BYPASS' | 'DYNAMIC' | 'EXPIRED'

interface TestConfig {
  bucketType: BucketType
  accessMethods: AccessMethod[]
}

interface TransformParams {
  width: number
  height: number
}

const onePixelPng = new Uint8Array(
  Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p94AAAAASUVORK5CYII=',
    'base64'
  )
)

const SIGNED_EXPIRES_IN_S = 7
const CACHE_RETRIES = 10
const TEST_CONFIGS: TestConfig[] = [
  { bucketType: 'public', accessMethods: ['public'] },
  { bucketType: 'private', accessMethods: ['authenticated', 'signed'] },
]

/**
 * Adds a pause conditionally for signed requests
 *
 * This must be placed after write operations (upload, upsert, move, copy) for signed requests
 * to ensure the webhook is processed before the first GET
 *
 * Without this we get 60s of EXPIRED or REVALIDATED headers due to KV cache
 */
async function pauseForWebhookIfNeeded(accessMethod: AccessMethod) {
  if (accessMethod === 'signed') {
    await delay(3000)
  }
}

/**
 * Get a route/token for an object based on bucket type and access method.
 */
async function getObjectUrl(
  bucketName: string,
  objectKey: string,
  bucketType: BucketType,
  accessMethod: AccessMethod,
  transform?: TransformParams
): Promise<{ route: string; token?: string }> {
  const config = getAcceptanceConfig()
  const encodedKey = encodePathSegments(objectKey)
  const routePrefix = transform ? 'render/image' : 'object'
  const query = transform ? `?width=${transform.width}&height=${transform.height}` : ''

  if (bucketType === 'public' && accessMethod === 'public') {
    return {
      route: `/${routePrefix}/public/${bucketName}/${encodedKey}${query}`,
    }
  }

  if (accessMethod === 'authenticated') {
    return {
      token: requireConfigValue(config.authenticatedKey, 'ACCEPTANCE_AUTHENTICATED_KEY'),
      route: `/${routePrefix}/authenticated/${bucketName}/${encodedKey}${query}`,
    }
  }

  if (accessMethod === 'signed') {
    const client = createRestClient()
    const signed = await client.request<SignedUrlResponse>(
      'POST',
      `/object/sign/${bucketName}/${encodedKey}`,
      {
        body: {
          expiresIn: SIGNED_EXPIRES_IN_S,
          ...(transform
            ? { transform: { height: transform.height, width: transform.width, resize: 'contain' } }
            : {}),
        },
        expectedStatus: 200,
        token: requireServiceKey(config),
      }
    )
    return {
      route: signed.json?.signedURL ?? '',
    }
  }

  throw new Error(`Invalid access method: ${accessMethod}`)
}

/**
 * Replaces the `token` query param on a signed route
 */
function withToken(route: string, token: string): string {
  const parsed = new URL(route, 'http://abc')
  parsed.searchParams.set('token', token)
  return parsed.pathname + parsed.search
}

/**
 * Returns the `token` query param from a signed route
 */
function getToken(route: string): string {
  return new URL(route, 'http://abc').searchParams.get('token') || ''
}

describeAcceptance(
  'CDN cache purge operations',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['cdn'],
  },
  () => {
    let client: AcceptanceHttpClient
    let bucketName: string
    const isCdnEdge = getAcceptanceConfig().capabilities.cdnEdge

    beforeAll(async () => {
      client = createRestClient()
      bucketName = uniqueBucketName('cdn-purge')
      await createRestBucket(bucketName, { isPublic: true })
    })

    afterAll(async () => {
      await cleanupRestResources(bucketName, [], client)
    })

    it('purges an object cache entry', async () => {
      const purgedKey = uniqueObjectKey('cdn-purge-target')
      const controlKey = uniqueObjectKey('cdn-purge-control')
      let purgedRoute = ''
      let controlRoute = ''

      try {
        if (isCdnEdge) {
          await uploadRestObject(bucketName, purgedKey, 'cdn-purge-target-content')
          await uploadRestObject(bucketName, controlKey, 'cdn-purge-control-content')

          purgedRoute = (await getObjectUrl(bucketName, purgedKey, 'public', 'public')).route
          controlRoute = (await getObjectUrl(bucketName, controlKey, 'public', 'public')).route

          await client.request('GET', purgedRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', purgedRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', controlRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', controlRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
        }

        const purge = await client.request<CdnPurgeResponse>(
          'DELETE',
          `/cdn/${bucketName}/${encodePathSegments(purgedKey)}`,
          {
            expectedStatus: 200,
            token: requireServiceKey(),
          }
        )
        expect(purge.json).toMatchObject({
          message: 'success',
        })

        if (isCdnEdge) {
          await client.request('GET', purgedRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', controlRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
          })
        }
      } finally {
        if (isCdnEdge) {
          await cleanupRestObjects(bucketName, [purgedKey, controlKey], client)
        }
      }
    })

    it('purges object transformations cache', async () => {
      const objectKey = uniqueObjectKey('cdn-purge', 'png')
      let objectRoute = ''
      let transformRoute = ''

      try {
        if (isCdnEdge) {
          await uploadRestObject(bucketName, objectKey, onePixelPng, {
            contentType: 'image/png',
          })

          objectRoute = (await getObjectUrl(bucketName, objectKey, 'public', 'public')).route
          transformRoute = (
            await getObjectUrl(bucketName, objectKey, 'public', 'public', {
              width: 10,
              height: 10,
            })
          ).route

          await client.request('GET', objectRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', objectRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', transformRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', transformRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
        }

        const purge = await client.request<CdnPurgeResponse>(
          'DELETE',
          `/cdn/${bucketName}/${encodePathSegments(objectKey)}?transformations=true`,
          {
            expectedStatus: 200,
            token: requireServiceKey(),
          }
        )
        expect(purge.json).toMatchObject({
          message: 'success',
        })

        if (isCdnEdge) {
          await client.request('GET', transformRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', objectRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
          })
        }
      } finally {
        if (isCdnEdge) {
          await cleanupRestObjects(bucketName, [objectKey], client)
        }
      }
    })

    it('purges an entire bucket cache', async () => {
      const objectKey = uniqueObjectKey('cdn-purge-bucket')
      const controlBucketName = uniqueBucketName('cdn-purge-bucket-control')
      const controlKey = uniqueObjectKey('cdn-purge-bucket-control')
      let route = ''
      let controlRoute = ''

      try {
        if (isCdnEdge) {
          await createRestBucket(controlBucketName, { isPublic: true })
          await uploadRestObject(bucketName, objectKey, 'cdn-purge-bucket-target')
          await uploadRestObject(controlBucketName, controlKey, 'cdn-purge-bucket-control')

          route = (await getObjectUrl(bucketName, objectKey, 'public', 'public')).route
          controlRoute = (await getObjectUrl(controlBucketName, controlKey, 'public', 'public'))
            .route

          await client.request('GET', route, { expectedCacheStatus: 'MISS', expectedStatus: 200 })
          await client.request('GET', route, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', controlRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', controlRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
        }

        const purge = await client.request<CdnPurgeResponse>('DELETE', `/cdn/${bucketName}`, {
          expectedStatus: 200,
          token: requireServiceKey(),
        })
        expect(purge.json).toMatchObject({
          message: 'success',
        })

        if (isCdnEdge) {
          await client.request('GET', route, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', controlRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
          })
        }
      } finally {
        if (isCdnEdge) {
          await cleanupRestResources(controlBucketName, [controlKey], client)
          await cleanupRestObjects(bucketName, [objectKey], client)
        }
      }
    })

    it('purges bucket transformations cache', async () => {
      const objectKey = uniqueObjectKey('cdn-purge-bucket', 'png')
      let objectRoute = ''
      let transformRoute = ''

      try {
        if (isCdnEdge) {
          await uploadRestObject(bucketName, objectKey, onePixelPng, {
            contentType: 'image/png',
          })

          objectRoute = (await getObjectUrl(bucketName, objectKey, 'public', 'public')).route
          transformRoute = (
            await getObjectUrl(bucketName, objectKey, 'public', 'public', {
              width: 10,
              height: 10,
            })
          ).route

          await client.request('GET', objectRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', objectRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', transformRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', transformRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
        }

        const purge = await client.request<CdnPurgeResponse>(
          'DELETE',
          `/cdn/${bucketName}?transformations=true`,
          {
            expectedStatus: 200,
            token: requireServiceKey(),
          }
        )
        expect(purge.json).toMatchObject({
          message: 'success',
        })

        if (isCdnEdge) {
          await client.request('GET', transformRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', objectRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
          })
        }
      } finally {
        if (isCdnEdge) {
          await cleanupRestObjects(bucketName, [objectKey], client)
        }
      }
    })

    it('purges entire tenant cache', async () => {
      const objectKey = uniqueObjectKey('cdn-purge-tenant')
      const secondBucketName = uniqueBucketName('cdn-purge-tenant')
      const secondKey = uniqueObjectKey('cdn-purge-tenant')
      let route = ''
      let secondRoute = ''

      try {
        if (isCdnEdge) {
          await createRestBucket(secondBucketName, { isPublic: true })
          await uploadRestObject(bucketName, objectKey, 'cdn-purge-tenant-target')
          await uploadRestObject(secondBucketName, secondKey, 'cdn-purge-tenant-target-2')

          route = (await getObjectUrl(bucketName, objectKey, 'public', 'public')).route
          secondRoute = (await getObjectUrl(secondBucketName, secondKey, 'public', 'public')).route

          await client.request('GET', route, { expectedCacheStatus: 'MISS', expectedStatus: 200 })
          await client.request('GET', route, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', secondRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', secondRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
        }

        const purge = await client.request<CdnPurgeResponse>('DELETE', '/cdn/', {
          expectedStatus: 200,
          token: requireServiceKey(),
        })
        expect(purge.json).toMatchObject({
          message: 'success',
        })

        if (isCdnEdge) {
          await client.request('GET', route, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', secondRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
        }
      } finally {
        if (isCdnEdge) {
          await cleanupRestResources(secondBucketName, [secondKey], client)
          await cleanupRestObjects(bucketName, [objectKey], client)
        }
      }
    })

    it('purges tenant transformations cache', async () => {
      const objectKey = uniqueObjectKey('cdn-purge-tenant', 'png')
      const secondBucketName = uniqueBucketName('cdn-purge-tenant')
      const secondKey = uniqueObjectKey('cdn-purge-tenant', 'png')
      let objectRoute = ''
      let transformRoute = ''
      let secondObjectRoute = ''
      let secondTransformRoute = ''

      try {
        if (isCdnEdge) {
          await createRestBucket(secondBucketName, { isPublic: true })
          await uploadRestObject(bucketName, objectKey, onePixelPng, { contentType: 'image/png' })
          await uploadRestObject(secondBucketName, secondKey, onePixelPng, {
            contentType: 'image/png',
          })

          objectRoute = (await getObjectUrl(bucketName, objectKey, 'public', 'public')).route
          transformRoute = (
            await getObjectUrl(bucketName, objectKey, 'public', 'public', {
              width: 10,
              height: 10,
            })
          ).route
          secondObjectRoute = (await getObjectUrl(secondBucketName, secondKey, 'public', 'public'))
            .route
          secondTransformRoute = (
            await getObjectUrl(secondBucketName, secondKey, 'public', 'public', {
              width: 10,
              height: 10,
            })
          ).route

          await client.request('GET', objectRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', objectRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', transformRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', transformRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', secondObjectRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', secondObjectRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', secondTransformRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
          })
          await client.request('GET', secondTransformRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
        }

        const purge = await client.request<CdnPurgeResponse>(
          'DELETE',
          '/cdn?transformations=true',
          {
            expectedStatus: 200,
            token: requireServiceKey(),
          }
        )
        expect(purge.json).toMatchObject({
          message: 'success',
        })

        if (isCdnEdge) {
          await client.request('GET', transformRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', secondTransformRoute, {
            expectedCacheStatus: 'MISS',
            expectedStatus: 200,
            retries: CACHE_RETRIES,
          })
          await client.request('GET', objectRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
          })
          await client.request('GET', secondObjectRoute, {
            expectedCacheStatus: 'HIT',
            expectedStatus: 200,
          })
        }
      } finally {
        if (isCdnEdge) {
          await cleanupRestResources(secondBucketName, [secondKey], client)
          await cleanupRestObjects(bucketName, [objectKey], client)
        }
      }
    })
  }
)

TEST_CONFIGS.forEach(({ bucketType, accessMethods }) => {
  accessMethods.forEach((accessMethod) => {
    const isPublic = bucketType === 'public'
    const isAuthenticated = accessMethod === 'authenticated'
    const isSigned = accessMethod === 'signed'
    const testLabel = `${bucketType} bucket with ${accessMethod} access`

    describeAcceptance(
      `CDN cache behavior - ${testLabel}`,
      {
        destructive: true,
        profiles: ['full'],
        requires: isAuthenticated ? ['cdnEdge', 'rlsSetup'] : ['cdnEdge'],
      },
      () => {
        let client: AcceptanceHttpClient
        let bucketName: string
        let writePrefix: string | undefined

        beforeAll(async () => {
          client = createRestClient()

          if (isAuthenticated) {
            // Authenticated access is governed by RLS, so reuse the shared RLS
            // bucket/prefix instead of provisioning a dedicated bucket.
            const config = getAcceptanceConfig()
            bucketName = requireConfigValue(config.rlsBucket, 'ACCEPTANCE_RLS_BUCKET')
            writePrefix = requireConfigValue(
              config.rlsWritePrefix,
              'ACCEPTANCE_RLS_WRITE_PREFIX'
            ).replace(/\/+$/, '')
          } else {
            bucketName = uniqueBucketName(`cdn-${bucketType}-${accessMethod}`)
            await createRestBucket(bucketName, { isPublic })
          }
        })

        afterAll(async () => {
          if (!isAuthenticated) {
            await cleanupRestResources(bucketName, [], client)
          }
        })

        function makeObjectKey(kind: string, extension = 'txt'): string {
          const key = uniqueObjectKey(kind, extension)
          return writePrefix ? `${writePrefix}/${key}` : key
        }

        it('supports basic cache MISS / HIT for multiple files', async () => {
          const files = [
            { key: makeObjectKey('file1'), content: 'Content of file 1' },
            { key: makeObjectKey('file2'), content: 'Content of file 2' },
            { key: makeObjectKey('file3'), content: 'Content of file 3' },
          ]

          try {
            const items = await Promise.all(
              files.map(async (file) => {
                await uploadRestObject(bucketName, file.key, file.content)
                const { route, token } = await getObjectUrl(
                  bucketName,
                  file.key,
                  bucketType,
                  accessMethod
                )
                return { file, route, token }
              })
            )

            await pauseForWebhookIfNeeded(accessMethod)

            // check invalid token / access denied
            if (isSigned) {
              // const { route } = items[0]
              const invalidRoute = withToken(items[0].route, getToken(items[1].route))
              const denied = await client.request('GET', invalidRoute, {
                expectedCacheStatus: 'BYPASS',
                expectedStatus: 400,
              })
              expect(parseStorageError(denied.json)).toMatchObject({
                statusCode: '400',
                error: 'InvalidSignature',
              })
            } else if (isAuthenticated) {
              const anonKey = requireConfigValue(
                getAcceptanceConfig().anonKey,
                'ACCEPTANCE_ANON_KEY'
              )
              const denied = await client.request('GET', items[0].route, {
                expectedCacheStatus: 'DYNAMIC',
                expectedStatus: 400,
                token: anonKey,
              })
              expect(parseStorageError(denied.json)).toMatchObject({
                statusCode: '404',
                error: 'not_found',
              })
            }

            for (let i = 0; i < items.length; i++) {
              const { file, route, token } = items[i]
              const first = await client.request('GET', route, {
                expectedCacheStatus: 'MISS',
                expectedStatus: 200,
                token,
              })
              expect(first.body).toBe(file.content)

              const second = await client.request('GET', route, {
                expectedCacheStatus: 'HIT',
                expectedStatus: 200,
                retries: CACHE_RETRIES,
                token,
              })
              expect(second.body).toBe(file.content)
            }
          } finally {
            await cleanupRestObjects(
              bucketName,
              files.map((f) => f.key),
              client
            )
          }
        })

        it('supports ETag-based conditional requests with 304 responses', async () => {
          const objectKey = makeObjectKey('etag')
          const content = 'etag-test-content'

          try {
            await uploadRestObject(bucketName, objectKey, content)

            await pauseForWebhookIfNeeded(accessMethod)

            const { route, token } = await getObjectUrl(
              bucketName,
              objectKey,
              bucketType,
              accessMethod
            )

            const first = await client.request('GET', route, {
              expectedCacheStatus: 'MISS',
              expectedStatus: 200,
              token,
            })
            const etag = first.headers.get('etag')
            expect(etag).toBeTruthy()

            const second = await client.request('GET', route, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 304,
              headers: { 'If-None-Match': etag! },
              token,
            })

            expect(second.body).toBe('')
          } finally {
            await cleanupRestObjects(bucketName, [objectKey], client)
          }
        })

        it('invalidates cache after UPSERT operation', async () => {
          const objectKey = makeObjectKey('upsert')
          const initialContent = 'initial-content'
          const updatedContent = 'updated-content'

          try {
            await uploadRestObject(bucketName, objectKey, initialContent)

            await pauseForWebhookIfNeeded(accessMethod)

            const { route, token } = await getObjectUrl(
              bucketName,
              objectKey,
              bucketType,
              accessMethod
            )

            await client.request('GET', route, {
              expectedCacheStatus: 'MISS',
              expectedStatus: 200,
              token,
            })
            await client.request('GET', route, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 200,
              token,
            })

            await uploadRestObject(bucketName, objectKey, updatedContent)

            await pauseForWebhookIfNeeded(accessMethod)

            const { route: newRoute, token: newToken } = await getObjectUrl(
              bucketName,
              objectKey,
              bucketType,
              accessMethod
            )

            const afterUpsert = await client.request('GET', newRoute, {
              expectedCacheStatus: 'MISS',
              expectedStatus: 200,
              retries: CACHE_RETRIES,
              token: newToken,
            })
            expect(afterUpsert.body).toBe(updatedContent)

            await client.request('GET', newRoute, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 200,
              retries: CACHE_RETRIES,
              token: newToken,
            })
          } finally {
            await cleanupRestObjects(bucketName, [objectKey], client)
          }
        })

        it('invalidates cache at old path and creates new cache after MOVE', async () => {
          const config = getAcceptanceConfig()
          const oldKey = makeObjectKey('move-old')
          const newKey = makeObjectKey('move-new')
          const content = 'move-test-content'

          try {
            await uploadRestObject(bucketName, oldKey, content)

            await pauseForWebhookIfNeeded(accessMethod)

            const { route: oldRoute, token: oldToken } = await getObjectUrl(
              bucketName,
              oldKey,
              bucketType,
              accessMethod
            )

            await client.request('GET', oldRoute, {
              expectedCacheStatus: 'MISS',
              expectedStatus: 200,
              token: oldToken,
            })
            await client.request('GET', oldRoute, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 200,
              retries: CACHE_RETRIES,
              token: oldToken,
            })

            await client.request('POST', '/object/move', {
              body: {
                bucketId: bucketName,
                destinationKey: newKey,
                sourceKey: oldKey,
              },
              expectedStatus: 200,
              token: requireServiceKey(config),
            })

            // Signed requests return HIT for 60s or until the token expires
            await delay(SIGNED_EXPIRES_IN_S * 1000)

            const expectedError = isSigned
              ? { statusCode: '400', error: 'InvalidJWT' }
              : { statusCode: '404', error: 'not_found' }
            const expectedCacheStatus: CacheStatus = isSigned ? 'BYPASS' : 'DYNAMIC'
            const oldResult = await client.request('GET', oldRoute, {
              expectedCacheStatus,
              expectedStatus: 400,
              retries: CACHE_RETRIES,
              token: oldToken,
            })
            expect(parseStorageError(oldResult.json)).toMatchObject(expectedError)

            await pauseForWebhookIfNeeded(accessMethod)

            const { route: newRoute, token: newToken } = await getObjectUrl(
              bucketName,
              newKey,
              bucketType,
              accessMethod
            )

            const newFirst = await client.request('GET', newRoute, {
              expectedCacheStatus: 'MISS',
              expectedStatus: 200,
              token: newToken,
            })
            expect(newFirst.body).toBe(content)

            await client.request('GET', newRoute, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 200,
              token: newToken,
            })
          } finally {
            await cleanupRestObjects(bucketName, [newKey], client)
          }
        })

        it('creates new cache entry for COPY destination without affecting source', async () => {
          const config = getAcceptanceConfig()
          const sourceKey = makeObjectKey('copy-src')
          const destKey = makeObjectKey('copy-dest')
          const content = 'copy-test-content'

          try {
            await uploadRestObject(bucketName, sourceKey, content)

            await pauseForWebhookIfNeeded(accessMethod)

            const { route: sourceRoute, token: sourceToken } = await getObjectUrl(
              bucketName,
              sourceKey,
              bucketType,
              accessMethod
            )

            await client.request('GET', sourceRoute, {
              expectedCacheStatus: 'MISS',
              expectedStatus: 200,
              token: sourceToken,
            })
            await client.request('GET', sourceRoute, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 200,
              token: sourceToken,
            })

            await client.request('POST', '/object/copy', {
              body: {
                bucketId: bucketName,
                destinationKey: destKey,
                sourceKey,
              },
              expectedStatus: 200,
              token: requireServiceKey(config),
            })

            await pauseForWebhookIfNeeded(accessMethod)

            const { route: destRoute, token: destToken } = await getObjectUrl(
              bucketName,
              destKey,
              bucketType,
              accessMethod
            )

            const destFirst = await client.request('GET', destRoute, {
              expectedCacheStatus: 'MISS',
              expectedStatus: 200,
              token: destToken,
            })
            expect(destFirst.body).toBe(content)

            await client.request('GET', destRoute, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 200,
              token: destToken,
            })
            await client.request('GET', sourceRoute, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 200,
              token: sourceToken,
            })
          } finally {
            await cleanupRestObjects(bucketName, [sourceKey, destKey], client)
          }
        })

        it('invalidates cache after DELETE operation', async () => {
          const objectKey = makeObjectKey('delete')
          const content = 'delete-test-content'

          try {
            await uploadRestObject(bucketName, objectKey, content)

            await pauseForWebhookIfNeeded(accessMethod)

            const { route, token } = await getObjectUrl(
              bucketName,
              objectKey,
              bucketType,
              accessMethod
            )

            await client.request('GET', route, {
              expectedCacheStatus: 'MISS',
              expectedStatus: 200,
              token,
            })
            await client.request('GET', route, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 200,
              token,
            })

            await client.request(
              'DELETE',
              `/object/${bucketName}/${encodePathSegments(objectKey)}`,
              {
                expectedStatus: 200,
                token: requireServiceKey(),
              }
            )

            // Signed requests return HIT for 60s or until the token expires
            await delay(SIGNED_EXPIRES_IN_S * 1000)

            const expectedError = isSigned
              ? { statusCode: '400', error: 'InvalidJWT' }
              : { statusCode: '404', error: 'not_found' }
            const expectedCacheStatus: CacheStatus = isSigned ? 'BYPASS' : 'DYNAMIC'
            const result = await client.request('GET', route, {
              expectedCacheStatus,
              expectedStatus: 400,
              retries: CACHE_RETRIES,
              token,
            })
            expect(parseStorageError(result.json)).toMatchObject(expectedError)
          } finally {
            await cleanupRestObjects(bucketName, [objectKey], client)
          }
        })

        it('caches different transformations independently', async () => {
          const objectKey = makeObjectKey('transform', 'png')

          try {
            await uploadRestObject(bucketName, objectKey, onePixelPng, {
              contentType: 'image/png',
            })

            await pauseForWebhookIfNeeded(accessMethod)

            const { route: transform1Route, token: token1 } = await getObjectUrl(
              bucketName,
              objectKey,
              bucketType,
              accessMethod,
              { width: 10, height: 10 }
            )
            const { route: transform2Route, token: token2 } = await getObjectUrl(
              bucketName,
              objectKey,
              bucketType,
              accessMethod,
              { width: 20, height: 20 }
            )

            await client.request('GET', transform1Route, {
              expectedCacheStatus: 'MISS',
              expectedStatus: 200,
              token: token1,
            })
            await client.request('GET', transform1Route, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 200,
              retries: CACHE_RETRIES,
              token: token1,
            })

            await client.request('GET', transform2Route, {
              expectedCacheStatus: 'MISS',
              expectedStatus: 200,
              token: token2,
            })
            await client.request('GET', transform2Route, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 200,
              retries: CACHE_RETRIES,
              token: token2,
            })

            await client.request('GET', transform1Route, {
              expectedCacheStatus: 'HIT',
              expectedStatus: 200,
              retries: CACHE_RETRIES,
              token: token1,
            })
          } finally {
            await cleanupRestObjects(bucketName, [objectKey], client)
          }
        })
      }
    )
  })
})
