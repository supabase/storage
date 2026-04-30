import { randomUUID } from 'node:crypto'
import { describeAcceptance, getAcceptanceConfig } from '../support/config'
import { createRestClient } from '../support/http'
import { requireServiceKey, uniqueBucketName } from '../support/resources'

interface IcebergBucket {
  id: string
  name: string
}

interface IcebergNamespaceList {
  namespaces?: string[][]
}

interface IcebergTableList {
  identifiers?: Array<{
    name: string
    namespace: string[]
  }>
}

interface IcebergCatalogConfig {
  defaults?: {
    prefix?: string
  }
  overrides?: {
    prefix?: string
  }
}

interface IcebergNamespaceResponse {
  namespace?: string[]
  properties?: Record<string, unknown>
}

interface IcebergTableResponse {
  metadata?: Record<string, unknown>
  'metadata-location'?: string
}

describeAcceptance(
  'Iceberg catalog contract',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['iceberg'],
  },
  () => {
    it('covers analytics buckets, catalog config, namespaces, and table lifecycle', async () => {
      const client = createRestClient()
      const token = requireServiceKey(getAcceptanceConfig())
      const bucketName = uniqueBucketName('ice')
      const suffix = randomUUID().replace(/-/g, '').slice(0, 12)
      const namespaceName = `ns${suffix}`
      const tableName = `tbl${suffix}`
      let namespaceCreated = false
      let tableCreated = false

      try {
        const createdBucket = await client.request<IcebergBucket>('POST', '/iceberg/bucket', {
          body: {
            name: bucketName,
          },
          expectedStatus: 200,
          token,
        })
        expect(createdBucket.json?.name).toBe(bucketName)

        const buckets = await client.request<IcebergBucket[]>(
          'GET',
          `/iceberg/bucket?search=${bucketName}`,
          {
            expectedStatus: 200,
            token,
          }
        )
        expect(buckets.json?.map((bucket) => bucket.name)).toContain(bucketName)

        const catalogConfig = await client.request<IcebergCatalogConfig>(
          'GET',
          `/iceberg/v1/config?warehouse=${bucketName}`,
          {
            expectedStatus: 200,
            token,
          }
        )
        expect(catalogConfig.json?.defaults?.prefix).toBe(bucketName)
        expect(catalogConfig.json?.overrides?.prefix).toBe(bucketName)

        const createdNamespace = await client.request<IcebergNamespaceResponse>(
          'POST',
          `/iceberg/v1/${bucketName}/namespaces`,
          {
            body: {
              namespace: namespaceName,
              properties: {
                purpose: 'acceptance',
              },
            },
            expectedStatus: 200,
            token,
          }
        )
        namespaceCreated = true
        expect(createdNamespace.json?.namespace).toEqual([namespaceName])

        await client.request('HEAD', `/iceberg/v1/${bucketName}/namespaces/${namespaceName}`, {
          expectedStatus: 204,
          token,
        })

        const namespace = await client.request<IcebergNamespaceResponse>(
          'GET',
          `/iceberg/v1/${bucketName}/namespaces/${namespaceName}`,
          {
            expectedStatus: 200,
            token,
          }
        )
        expect(namespace.json?.namespace).toEqual([namespaceName])

        const namespaces = await client.request<IcebergNamespaceList>(
          'GET',
          `/iceberg/v1/${bucketName}/namespaces`,
          {
            expectedStatus: 200,
            token,
          }
        )
        expect(namespaces.json?.namespaces?.flat()).toContain(namespaceName)

        const createdTable = await client.request<IcebergTableResponse>(
          'POST',
          `/iceberg/v1/${bucketName}/namespaces/${namespaceName}/tables`,
          {
            body: {
              name: tableName,
              properties: {
                purpose: 'acceptance',
              },
              schema: {
                fields: [
                  {
                    id: 1,
                    name: 'id',
                    required: false,
                    type: 'long',
                  },
                  {
                    id: 2,
                    name: 'name',
                    required: false,
                    type: 'string',
                  },
                ],
                'identifier-field-ids': [],
                'schema-id': 0,
                type: 'struct',
              },
              spec: {
                fields: [],
                'spec-id': 0,
              },
              'stage-create': false,
              'write-order': {
                fields: [],
                'order-id': 0,
              },
            },
            expectedStatus: 200,
            token,
          }
        )
        tableCreated = true
        expect(createdTable.json?.metadata).toBeTruthy()
        expect(createdTable.json?.['metadata-location']).toBeTruthy()

        const tables = await client.request<IcebergTableList>(
          'GET',
          `/iceberg/v1/${bucketName}/namespaces/${namespaceName}/tables`,
          {
            expectedStatus: 200,
            token,
          }
        )
        expect(tables.json?.identifiers?.map((table) => table.name)).toContain(tableName)

        const table = await client.request<IcebergTableResponse>(
          'GET',
          `/iceberg/v1/${bucketName}/namespaces/${namespaceName}/tables/${tableName}`,
          {
            expectedStatus: 200,
            token,
          }
        )
        expect(table.json?.metadata).toBeTruthy()
        expect(table.json?.['metadata-location']).toBeTruthy()

        await client.request(
          'HEAD',
          `/iceberg/v1/${bucketName}/namespaces/${namespaceName}/tables/${tableName}`,
          {
            expectedStatus: 204,
            token,
          }
        )

        const committed = await client.request<IcebergTableResponse>(
          'POST',
          `/iceberg/v1/${bucketName}/namespaces/${namespaceName}/tables/${tableName}`,
          {
            body: {
              requirements: [],
              updates: [
                {
                  action: 'set-properties',
                  updates: { 'acceptance.commit': 'true' },
                },
              ],
            },
            expectedStatus: 200,
            token,
          }
        )
        expect(committed.json?.metadata).toBeTruthy()

        await client.request('GET', `/iceberg/v1/${bucketName}/namespaces/missing${suffix}`, {
          expectedStatus: 404,
          token,
        })

        await client.request(
          'GET',
          `/iceberg/v1/${bucketName}/namespaces/${namespaceName}/tables/missing${suffix}`,
          {
            expectedStatus: 404,
            token,
          }
        )

        await client.request(
          'POST',
          `/iceberg/v1/${bucketName}/namespaces/${namespaceName}/tables`,
          {
            body: {
              name: tableName,
              schema: {
                type: 'struct',
                fields: [
                  {
                    id: 1,
                    name: 'id',
                    required: false,
                    type: 'long',
                  },
                ],
              },
            },
            expectedStatus: 409,
            token,
          }
        )
      } finally {
        if (tableCreated) {
          await client
            .request(
              'DELETE',
              `/iceberg/v1/${bucketName}/namespaces/${namespaceName}/tables/${tableName}`,
              {
                expectedStatus: [204, 400, 404],
                token,
              }
            )
            .catch(() => undefined)
        }

        if (namespaceCreated) {
          await client
            .request('DELETE', `/iceberg/v1/${bucketName}/namespaces/${namespaceName}`, {
              expectedStatus: [204, 400, 404],
              token,
            })
            .catch(() => undefined)
        }

        await client
          .request('DELETE', `/iceberg/bucket/${bucketName}`, {
            expectedStatus: [200, 400, 404],
            token,
          })
          .catch(() => undefined)
      }
    })
  }
)
