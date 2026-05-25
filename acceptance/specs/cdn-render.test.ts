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
      const missingObjectKey = uniqueObjectKey('cdn-missing')

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
        })

        const missingPurge = await client.request(
          'DELETE',
          `/cdn/${bucketName}/${encodePathSegments(missingObjectKey)}`,
          {
            expectedStatus: [400, 404],
            token: requireServiceKey(),
          }
        )
        expect(missingPurge.json).toBeTruthy()
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
            `/render/image/public/${bucketName}/${encodePathSegments(
              objectKey
            )}?width=1&height=1&format=webp`
          ),
          undefined,
          'image/webp'
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

    it('rejects non-image input and invalid transformations', async () => {
      const config = getAcceptanceConfig()
      const client = createRestClient()
      const bucketName = uniqueBucketName('render-errors')
      const objectKey = uniqueObjectKey('render-text', 'txt')

      try {
        await createRestBucket(bucketName, { isPublic: true })
        await uploadRestObject(bucketName, objectKey, 'not an image', {
          contentType: 'text/plain',
        })

        const invalidTransform = await fetchRenderedImage(
          joinUrl(
            config.baseUrl,
            `/render/image/public/${bucketName}/${encodePathSegments(objectKey)}?width=-1`
          )
        )
        expect(invalidTransform.status).toBe(400)
        expect(invalidTransform.contentType).not.toMatch(/^image\//)
        expect(invalidTransform.bodyText).toContain('width')

        const nonImage = await fetchRenderedImage(
          joinUrl(
            config.baseUrl,
            `/render/image/public/${bucketName}/${encodePathSegments(objectKey)}?width=1&height=1`
          )
        )
        expect([400, 415, 422]).toContain(nonImage.status)
        expect(nonImage.contentType).not.toMatch(/^image\//)
        expect(nonImage.bodyText.length).toBeGreaterThan(0)
      } finally {
        await cleanupRestResources(bucketName, [objectKey], client)
      }
    })
  }
)

async function expectRenderedImage(url: string, token?: string, expectedContentType?: string) {
  const rendered = await fetchRenderedImage(url, token)
  const failureBody = rendered.contentType.startsWith('image/') ? '' : rendered.bodyText

  expect(rendered.status, failureBody).toBe(200)
  if (expectedContentType) {
    expect(rendered.contentType, failureBody).toBe(expectedContentType)
  } else {
    expect(rendered.contentType, failureBody).toMatch(/^image\//)
  }
  expect(rendered.body.byteLength, failureBody).toBeGreaterThan(0)
}

async function fetchRenderedImage(url: string, token?: string) {
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

    const contentType = response.headers.get('content-type') ?? ''
    const body = await response.arrayBuffer()
    const bodyText = /^image\//.test(contentType) ? '' : new TextDecoder().decode(body)

    return {
      body,
      bodyText,
      contentType,
      status: response.status,
    }
  } finally {
    if (response && !response.bodyUsed) {
      await response.body?.cancel()
    }
  }
}
