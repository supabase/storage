import { randomUUID } from 'node:crypto'
import { describeAcceptance, getAcceptanceConfig } from '../support/config'
import { createRestClient } from '../support/http'
import { requireServiceKey } from '../support/resources'

interface VectorListBucketsResponse {
  nextToken?: string
  vectorBuckets?: Array<{ vectorBucketName?: string }>
}

interface VectorListIndexesResponse {
  indexes?: Array<{ indexName?: string }>
  nextToken?: string
}

interface VectorListResponse {
  vectors?: Array<{ key?: string }>
}

describeAcceptance(
  'vector API contract',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['vector'],
  },
  () => {
    it('covers vector bucket, index, put/get/list/query/delete lifecycle', async () => {
      const config = getAcceptanceConfig()
      const client = createRestClient()
      const token = requireServiceKey(config)
      const suffix = randomUUID().replace(/-/g, '').slice(0, 12)
      const vectorPrefix = `${config.resourcePrefix.slice(0, 24)}-vec-${suffix}`.slice(0, 41)
      const vectorBucketName = `${vectorPrefix}-a`
      const secondaryVectorBucketName = `${vectorPrefix}-b`
      const indexName = `idx-${suffix}-a`
      const secondaryIndexName = `idx-${suffix}-b`
      const vectorKeys = [`vec-a-${suffix}`, `vec-b-${suffix}`]

      try {
        await client.request('POST', '/vector/CreateVectorBucket', {
          body: {
            vectorBucketName,
          },
          expectedStatus: 200,
          token,
        })

        await client.request('POST', '/vector/CreateVectorBucket', {
          body: {
            vectorBucketName: secondaryVectorBucketName,
          },
          expectedStatus: 200,
          token,
        })

        const buckets = await client.request<VectorListBucketsResponse>(
          'POST',
          '/vector/ListVectorBuckets',
          {
            body: {
              maxResults: 1,
              prefix: vectorPrefix,
            },
            expectedStatus: 200,
            token,
          }
        )
        expect(buckets.json?.vectorBuckets).toHaveLength(1)
        expect(buckets.json?.nextToken).toBeTruthy()

        const bucketsSecondPage = await client.request<VectorListBucketsResponse>(
          'POST',
          '/vector/ListVectorBuckets',
          {
            body: {
              maxResults: 1,
              nextToken: buckets.json?.nextToken,
              prefix: vectorPrefix,
            },
            expectedStatus: 200,
            token,
          }
        )
        const listedBucketNames = [
          ...(buckets.json?.vectorBuckets ?? []),
          ...(bucketsSecondPage.json?.vectorBuckets ?? []),
        ].map((bucket) => bucket.vectorBucketName)
        expect(listedBucketNames).toEqual(
          expect.arrayContaining([vectorBucketName, secondaryVectorBucketName])
        )

        await client.request('POST', '/vector/GetVectorBucket', {
          body: {
            vectorBucketName,
          },
          expectedStatus: 200,
          token,
        })

        await client.request('POST', '/vector/CreateIndex', {
          body: {
            dataType: 'float32',
            dimension: 2,
            distanceMetric: 'cosine',
            indexName,
            vectorBucketName,
          },
          expectedStatus: 200,
          token,
        })

        await client.request('POST', '/vector/CreateIndex', {
          body: {
            dataType: 'float32',
            dimension: 2,
            distanceMetric: 'cosine',
            indexName: secondaryIndexName,
            vectorBucketName,
          },
          expectedStatus: 200,
          token,
        })

        const indexes = await client.request<VectorListIndexesResponse>(
          'POST',
          '/vector/ListIndexes',
          {
            body: {
              maxResults: 1,
              prefix: `idx-${suffix}`,
              vectorBucketName,
            },
            expectedStatus: 200,
            token,
          }
        )
        expect(indexes.json?.indexes).toHaveLength(1)
        expect(indexes.json?.nextToken).toBeTruthy()

        const indexesSecondPage = await client.request<VectorListIndexesResponse>(
          'POST',
          '/vector/ListIndexes',
          {
            body: {
              maxResults: 1,
              nextToken: indexes.json?.nextToken,
              prefix: `idx-${suffix}`,
              vectorBucketName,
            },
            expectedStatus: 200,
            token,
          }
        )
        const listedIndexNames = [
          ...(indexes.json?.indexes ?? []),
          ...(indexesSecondPage.json?.indexes ?? []),
        ].map((index) => index.indexName)
        expect(listedIndexNames).toEqual(expect.arrayContaining([indexName, secondaryIndexName]))

        await client.request('POST', '/vector/GetIndex', {
          body: {
            indexName,
            vectorBucketName,
          },
          expectedStatus: 200,
          token,
        })

        await client.request('POST', '/vector/PutVectors', {
          body: {
            indexName,
            vectorBucketName,
            vectors: [
              {
                data: {
                  float32: [1, 0],
                },
                key: vectorKeys[0],
                metadata: {
                  group: 'acceptance',
                },
              },
              {
                data: {
                  float32: [0, 1],
                },
                key: vectorKeys[1],
                metadata: {
                  group: 'acceptance',
                },
              },
            ],
          },
          expectedStatus: 200,
          token,
        })

        const vectors = await client.request<VectorListResponse>('POST', '/vector/ListVectors', {
          body: {
            indexName,
            returnData: true,
            returnMetadata: true,
            vectorBucketName,
          },
          expectedStatus: 200,
          token,
        })
        expect(vectors.json?.vectors?.map((vector) => vector.key)).toEqual(
          expect.arrayContaining(vectorKeys)
        )

        const fetched = await client.request<VectorListResponse>('POST', '/vector/GetVectors', {
          body: {
            indexName,
            keys: vectorKeys,
            returnData: true,
            returnMetadata: true,
            vectorBucketName,
          },
          expectedStatus: 200,
          token,
        })
        expect(fetched.json?.vectors?.map((vector) => vector.key)).toEqual(
          expect.arrayContaining(vectorKeys)
        )

        const query = await client.request<VectorListResponse>('POST', '/vector/QueryVectors', {
          body: {
            filter: {
              group: 'acceptance',
            },
            indexName,
            queryVector: {
              float32: [1, 0],
            },
            returnDistance: true,
            returnMetadata: true,
            topK: 2,
            vectorBucketName,
          },
          expectedStatus: 200,
          token,
        })
        expect(query.json?.vectors?.map((vector) => vector.key)).toContain(vectorKeys[0])

        await client.request('POST', '/vector/DeleteVectors', {
          body: {
            indexName,
            keys: vectorKeys,
            vectorBucketName,
          },
          expectedStatus: 200,
          token,
        })
      } finally {
        await client
          .request('POST', '/vector/DeleteIndex', {
            body: {
              indexName,
              vectorBucketName,
            },
            expectedStatus: [200, 400, 404],
            token,
          })
          .catch(() => undefined)
        await client
          .request('POST', '/vector/DeleteIndex', {
            body: {
              indexName: secondaryIndexName,
              vectorBucketName,
            },
            expectedStatus: [200, 400, 404],
            token,
          })
          .catch(() => undefined)
        await client
          .request('POST', '/vector/DeleteVectorBucket', {
            body: {
              vectorBucketName,
            },
            expectedStatus: [200, 400, 404],
            token,
          })
          .catch(() => undefined)
        await client
          .request('POST', '/vector/DeleteVectorBucket', {
            body: {
              vectorBucketName: secondaryVectorBucketName,
            },
            expectedStatus: [200, 400, 404],
            token,
          })
          .catch(() => undefined)
      }
    })
  }
)
