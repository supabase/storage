import {
  describeAcceptance,
  encodePathSegments,
  getAcceptanceConfig,
  joinUrl,
} from '../support/config'
import { createAcceptanceHeaders, createRestClient } from '../support/http'
import {
  cleanupRestResources,
  createRestBucket,
  requireServiceKey,
  uniqueBucketName,
  uniqueObjectKey,
  uploadRestObject,
} from '../support/resources'

interface SignedUrlResponse {
  signedURL: string
}

interface CdnPurgeResponse {
  message: string
  statusCode: string
}

const onePixelPng = new Uint8Array(
  Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p94AAAAASUVORK5CYII=',
    'base64'
  )
)

describeAcceptance(
  'CDN cache contract',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['cdn'],
  },
  () => {
    it('purges an object cache entry', async () => {
      const client = createRestClient()
      const bucketName = uniqueBucketName('cdn')
      const objectKey = uniqueObjectKey('cdn')

      try {
        await createRestBucket(bucketName, { isPublic: true })
        await uploadRestObject(bucketName, objectKey, 'cdn-purge-target')

        const purge = await client.request<CdnPurgeResponse>(
          'DELETE',
          `/cdn/${bucketName}/${encodePathSegments(objectKey)}`,
          {
            expectedStatus: 200,
            token: requireServiceKey(),
          }
        )
        expect(purge.json).toMatchObject({
          message: 'success',
          statusCode: '200',
        })
      } finally {
        await cleanupRestResources(bucketName, [objectKey], client)
      }
    })
  }
)

describeAcceptance(
  'image rendering contract',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['render'],
  },
  () => {
    it('renders public, authenticated, and signed transformed images', async () => {
      const config = getAcceptanceConfig()
      const client = createRestClient()
      const token = requireServiceKey(config)
      const bucketName = uniqueBucketName('render')
      const objectKey = uniqueObjectKey('render', 'png')

      try {
        await createRestBucket(bucketName, { isPublic: true })
        await uploadRestObject(bucketName, objectKey, onePixelPng, {
          contentType: 'image/png',
        })

        await expectRenderedImage(
          joinUrl(
            config.baseUrl,
            `/render/image/public/${bucketName}/${encodePathSegments(objectKey)}?width=1&height=1`
          )
        )

        await expectRenderedImage(
          joinUrl(
            config.baseUrl,
            `/render/image/authenticated/${bucketName}/${encodePathSegments(objectKey)}?width=1&height=1`
          ),
          token
        )

        const signed = await client.request<SignedUrlResponse>(
          'POST',
          `/object/sign/${bucketName}/${encodePathSegments(objectKey)}`,
          {
            body: {
              expiresIn: 60,
              transform: {
                height: 1,
                width: 1,
              },
            },
            expectedStatus: 200,
            token,
          }
        )
        const signedUrl = new URL(joinUrl(config.baseUrl, signed.json?.signedURL ?? ''))
        const signedToken = signedUrl.searchParams.get('token')
        expect(signedToken).toBeTruthy()

        await expectRenderedImage(
          joinUrl(
            config.baseUrl,
            `/render/image/sign/${bucketName}/${encodePathSegments(objectKey)}?token=${encodeURIComponent(
              signedToken ?? ''
            )}`
          )
        )
      } finally {
        await cleanupRestResources(bucketName, [objectKey], client)
      }
    })
  }
)

async function expectRenderedImage(url: string, token?: string) {
  let response: Response | undefined
  try {
    response = await fetch(url, {
      headers: createAcceptanceHeaders(
        token
          ? {
              authorization: `Bearer ${token}`,
            }
          : undefined
      ),
    })

    expect(response.status).toBe(200)
    expect(response.headers.get('content-type')).toMatch(/^image\//)
    expect((await response.arrayBuffer()).byteLength).toBeGreaterThan(0)
  } finally {
    if (response && !response.bodyUsed) {
      await response.body?.cancel()
    }
  }
}
