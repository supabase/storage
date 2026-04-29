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

interface BucketResponse {
  allowed_mime_types?: string[] | null
  file_size_limit?: number | string | null
  id: string
  name: string
  public: boolean
}

interface ObjectInfoResponse {
  bucket_id?: string
  name?: string
}

interface SignedUrlBatchItem {
  error: string | null
  path: string
  signedURL: string | null
}

interface SignedUploadResponse {
  token: string
  url: string
}

interface ListObjectsV1Item {
  name: string
}

describeAcceptance(
  'extended REST bucket and object contract',
  {
    destructive: true,
    profiles: ['core'],
  },
  () => {
    it('empties and deletes an empty bucket', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('empty')

      try {
        await createRestBucket(bucketName)
        await client.request('POST', `/bucket/${bucketName}/empty`, {
          expectedStatus: 200,
          token,
        })
        await client.request('DELETE', `/bucket/${bucketName}`, {
          expectedStatus: 200,
          token,
        })
      } finally {
        await cleanupRestResources(bucketName, [], client)
      }
    })

    it('covers bucket metadata, public/info reads, signed upload, copy, move, update, bulk delete, and list-v1', async () => {
      const config = getAcceptanceConfig()
      const client = createRestClient()
      const token = requireServiceKey(config)
      const bucketName = uniqueBucketName('restx')
      const objectKey = uniqueObjectKey('restx')
      const signedUploadKey = uniqueObjectKey('signed-upload')
      const copyKey = uniqueObjectKey('copy')
      const movedKey = uniqueObjectKey('moved')
      const payload = `acceptance-rest-extended-${config.runId}`
      const updatedPayload = `${payload}-updated`
      const signedPayload = `${payload}-signed`
      const trackedKeys = [objectKey, signedUploadKey, copyKey, movedKey]

      try {
        await createRestBucket(bucketName, { isPublic: false })

        const bucket = await client.request<BucketResponse>('GET', `/bucket/${bucketName}`, {
          expectedStatus: 200,
          token,
        })
        expect(bucket.json?.id).toBe(bucketName)
        expect(bucket.json?.public).toBe(false)

        const listedBuckets = await client.request<BucketResponse[]>(
          'GET',
          `/bucket?search=${bucketName}`,
          {
            expectedStatus: 200,
            token,
          }
        )
        expect(listedBuckets.json?.map((listed) => listed.id)).toContain(bucketName)

        await client.request('PUT', `/bucket/${bucketName}`, {
          body: {
            allowed_mime_types: ['text/plain'],
            file_size_limit: 1_000_000,
            public: true,
          },
          expectedStatus: 200,
          token,
        })

        await uploadRestObject(bucketName, objectKey, payload)

        const publicRead = await client.request(
          'GET',
          `/object/public/${bucketName}/${encodePathSegments(objectKey)}`,
          { expectedStatus: 200 }
        )
        expect(publicRead.body).toBe(payload)

        const publicHead = await client.request(
          'HEAD',
          `/object/public/${bucketName}/${encodePathSegments(objectKey)}`,
          { expectedStatus: 200 }
        )
        expect(Number(publicHead.headers.get('content-length'))).toBe(payload.length)

        const publicInfo = await client.request<ObjectInfoResponse>(
          'GET',
          `/object/info/public/${bucketName}/${encodePathSegments(objectKey)}`,
          { expectedStatus: 200 }
        )
        expect(publicInfo.json?.name).toBe(objectKey)

        const authInfo = await client.request<ObjectInfoResponse>(
          'GET',
          `/object/info/authenticated/${bucketName}/${encodePathSegments(objectKey)}`,
          { expectedStatus: 200, token }
        )
        expect(authInfo.json?.bucket_id).toBe(bucketName)

        await client.request('HEAD', `/object/${bucketName}/${encodePathSegments(objectKey)}`, {
          expectedStatus: 200,
          token,
        })

        const objectInfo = await client.request<ObjectInfoResponse>(
          'GET',
          `/object/info/${bucketName}/${encodePathSegments(objectKey)}`,
          { expectedStatus: 200, token }
        )
        expect(objectInfo.json?.name).toBe(objectKey)

        const legacyList = await client.request<ListObjectsV1Item[]>(
          'POST',
          `/object/list/${bucketName}`,
          {
            body: {
              limit: 100,
              prefix: `${config.resourcePrefix}/`,
              sortBy: {
                column: 'name',
                order: 'asc',
              },
            },
            expectedStatus: 200,
            token,
          }
        )
        expect(legacyList.json?.map((object) => object.name)).toContain(
          objectKey.replace(`${config.resourcePrefix}/`, '')
        )

        const batchSigned = await client.request<SignedUrlBatchItem[]>(
          'POST',
          `/object/sign/${bucketName}`,
          {
            body: {
              expiresIn: 60,
              paths: [objectKey, `${objectKey}.missing`],
            },
            expectedStatus: 200,
            token,
          }
        )
        expect(batchSigned.json?.find((item) => item.path === objectKey)?.signedURL).toBeTruthy()

        const signedUpload = await client.request<SignedUploadResponse>(
          'POST',
          `/object/upload/sign/${bucketName}/${encodePathSegments(signedUploadKey)}`,
          {
            expectedStatus: 200,
            headers: {
              'content-type': 'text/plain',
              'x-upsert': 'true',
            },
            token,
          }
        )
        expect(signedUpload.json?.token).toBeTruthy()

        let signedUploadResponse: Response | undefined
        try {
          signedUploadResponse = await fetch(
            joinUrl(config.baseUrl, signedUpload.json?.url ?? ''),
            {
              body: signedPayload,
              headers: createAcceptanceHeaders({
                'content-type': 'text/plain',
              }),
              method: 'PUT',
            }
          )
          expect(signedUploadResponse.status).toBe(200)
        } finally {
          await signedUploadResponse?.body?.cancel()
        }

        await client.request('PUT', `/object/${bucketName}/${encodePathSegments(objectKey)}`, {
          body: updatedPayload,
          expectedStatus: 200,
          headers: {
            'content-type': 'text/plain',
          },
          token,
        })

        const updated = await client.request(
          'GET',
          `/object/authenticated/${bucketName}/${encodePathSegments(objectKey)}`,
          { expectedStatus: 200, token }
        )
        expect(updated.body).toBe(updatedPayload)

        await client.request('POST', '/object/copy', {
          body: {
            bucketId: bucketName,
            destinationKey: copyKey,
            sourceKey: objectKey,
          },
          expectedStatus: 200,
          headers: {
            'x-upsert': 'true',
          },
          token,
        })

        await client.request('POST', '/object/move', {
          body: {
            bucketId: bucketName,
            destinationKey: movedKey,
            sourceKey: copyKey,
          },
          expectedStatus: 200,
          token,
        })

        const moved = await client.request(
          'GET',
          `/object/authenticated/${bucketName}/${encodePathSegments(movedKey)}`,
          { expectedStatus: 200, token }
        )
        expect(moved.body).toBe(updatedPayload)

        const deleted = await client.request<unknown[]>('DELETE', `/object/${bucketName}`, {
          body: {
            prefixes: [signedUploadKey, movedKey],
          },
          expectedStatus: 200,
          token,
        })
        expect(deleted.json?.length).toBe(2)

        await client.request('DELETE', `/object/${bucketName}/${encodePathSegments(objectKey)}`, {
          expectedStatus: 200,
          token,
        })
      } finally {
        await cleanupRestResources(bucketName, trackedKeys, client)
      }
    })
  }
)
