import { createBucketIfNotExists, useStorage } from './utils/storage'
import makeApp from '../app'
import { getConfig } from '../config'
import {
  CreateTableResponse,
  LoadTableResult,
  RestCatalogClient,
} from '@storage/protocols/iceberg/catalog'
import { FastifyInstance } from 'fastify'
import { KnexMetastore, Metastore } from '@storage/protocols/iceberg/knex'
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import assert from 'node:assert'
import { isS3Error } from '@internal/errors'

const {
  serviceKeyAsync,
  storageS3Region,
  storageS3Endpoint,
  s3ProtocolAccessKeyId,
  s3ProtocolAccessKeySecret,
} = getConfig()

describe('Iceberg Catalog', () => {
  const t = useStorage()

  let app: FastifyInstance
  let icebergMetastore: Metastore
  beforeAll(() => {
    app = makeApp()
    icebergMetastore = new KnexMetastore(t.database.connection.pool.acquire(), {
      multiTenant: false,
      schema: 'storage',
    })
  })

  afterEach(async () => {
    jest.restoreAllMocks()
  })

  afterAll(async () => {
    await app.close()
    await t.database.connection.pool.destroy()
  })

  it('can create a table bucket', async () => {
    const bucketName = t.random.name('ice-bucket')

    const response = await app.inject({
      method: 'POST',
      url: '/bucket',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${await serviceKeyAsync}`,
      },
      payload: {
        name: bucketName,
        type: 'ANALYTICS',
      },
    })

    const resp = await response.json()
    expect(response.statusCode).toBe(200)
    expect(resp.name).toBe(bucketName)
  })

  it('can get catalog config', async () => {
    const bucketName = t.random.name('ice-bucket')
    await t.storage.createIcebergBucket({
      name: bucketName,
      id: bucketName,
    })

    jest.spyOn(RestCatalogClient.prototype, 'getConfig').mockResolvedValue(
      Promise.resolve({
        defaults: {
          prefix: bucketName,
        },
        overrides: {
          prefix: bucketName,
        },
      })
    )

    const response = await app.inject({
      method: 'GET',
      url: `/iceberg/v1/config?warehouse=${bucketName}`,
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })

    const resp = await response.json()
    expect(response.statusCode).toBe(200)
    expect(resp.defaults).toEqual({
      prefix: bucketName,
    })
    expect(resp.overrides).toEqual({
      prefix: bucketName,
    })
  })

  describe('Namespace', () => {
    it('can create namespaces', async () => {
      const bucketName = t.random.name('ice-bucket')
      await t.storage.createIcebergBucket({
        name: bucketName,
        id: bucketName,
      })

      const namespaceName = t.random.name('namespace')

      jest.spyOn(RestCatalogClient.prototype, 'createNamespace').mockResolvedValue(
        Promise.resolve({
          namespace: [namespaceName],
          properties: {
            test: 'hello',
          },
        })
      )

      const response = await app.inject({
        method: 'POST',
        url: `/iceberg/v1/${bucketName}/namespaces`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
        payload: {
          namespace: [namespaceName],
          properties: {
            test: 'hello',
          },
        },
      })

      expect(response.statusCode).toBe(200)
      const resp = await response.json()
      expect(resp).toEqual({
        namespace: [namespaceName],
        properties: {
          test: 'hello',
        },
      })
    })

    it('can list namespaces', async () => {
      const bucketName = t.random.name('ice-bucket')

      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
        id: bucketName,
      })

      const namespaceName = t.random.name('namespace')

      const namespace = await icebergMetastore.assignNamespace({
        name: namespaceName,
        bucketId: bucket.id,
        tenantId: '',
      })

      jest.spyOn(RestCatalogClient.prototype, 'listNamespaces').mockResolvedValue(
        Promise.resolve({
          namespaces: [[namespaceName]],
        })
      )

      const response = await app.inject({
        method: 'GET',
        url: `/iceberg/v1/${bucketName}/namespaces`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })

      expect(response.statusCode).toBe(200)
      const resp = await response.json()
      expect(resp).toEqual({
        namespaces: [[namespace.name]],
      })
    })

    it('can drop namespaces', async () => {
      const bucketName = t.random.name('ice-bucket')

      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
        id: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.assignNamespace({
        name: namespaceName,
        bucketId: bucket.id,
      })

      const initialNamespaces = await icebergMetastore.listNamespaces({
        bucketId: bucket.id,
      })

      jest.spyOn(RestCatalogClient.prototype, 'dropNamespace').mockResolvedValue(Promise.resolve())

      const response = await app.inject({
        method: 'DELETE',
        url: `/iceberg/v1/${bucketName}/namespaces/${namespace.name}`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })

      expect(response.statusCode).toBe(204)
      const resp = response.body
      expect(resp).toEqual('')

      const afterDropNamespaces = await icebergMetastore.listNamespaces({
        tenantId: '',
        bucketId: bucket.id,
      })

      expect(afterDropNamespaces.length).toEqual(initialNamespaces.length - 1)
    })

    it('can load namespace metadata', async () => {
      const bucketName = t.random.name('ice-bucket')

      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
        id: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.assignNamespace({
        name: namespaceName,
        bucketId: bucket.id,
      })

      jest.spyOn(RestCatalogClient.prototype, 'loadNamespaceMetadata').mockResolvedValue(
        Promise.resolve({
          namespace: [namespace.name],
          properties: {
            test: 'hello',
          },
        })
      )

      const response = await app.inject({
        method: 'GET',
        url: `/iceberg/v1/${bucketName}/namespaces/${namespace.name}`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })

      expect(response.statusCode).toBe(200)
      const resp = await response.json()
      expect(resp).toEqual({
        namespace: [namespace.name],
        properties: {
          test: 'hello',
        },
      })
    })

    it('check if namespace exists', async () => {
      const bucketName = t.random.name('ice-bucket')

      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
        id: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.assignNamespace({
        name: namespaceName,
        bucketId: bucket.id,
      })

      jest
        .spyOn(RestCatalogClient.prototype, 'namespaceExists')
        .mockResolvedValue(Promise.resolve())

      const response = await app.inject({
        method: 'HEAD',
        url: `/iceberg/v1/${bucketName}/namespaces/${namespace.name}`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })

      expect(response.statusCode).toBe(204)
    })
  })

  describe('Table', () => {
    it('can create a table', async () => {
      const bucketName = t.random.name('ice-bucket')
      await t.storage.createIcebergBucket({
        name: bucketName,
        id: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.assignNamespace({
        name: namespaceName,
        bucketId: bucketName,
      })

      const tableName = t.random.name('table')
      const loadTable: CreateTableResponse = {
        metadata: {
          'current-schema-id': 0,
          'default-spec-id': 0,
          'format-version': 2,
          'last-column-id': 2,
          'last-updated-ms': 1752062419286,
          location: 's3://821c6598-0032-4345-h1sokpwnexm17nfi7n1axwxmnncg1aps1b--table-s3',
          'metadata-log': Array.from([]),
          'partition-specs': Array.from([
            {
              fields: Array.from([]),
              'spec-id': 0,
            },
          ]),
          properties: {
            'write.parquet.compression-codec': 'zstd',
          },
          schemas: Array.from([
            {
              fields: Array.from([
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
              ]),
              'schema-id': 0,
              type: 'struct',
            },
          ]),
          'table-uuid': 'b734865e-f9a5-4654-8b0b-e15683fcb195',
        },
        'metadata-location':
          's3://821c6598-0032-4345-h1sokpwnexm17nfi7n1axwxmnncg1aps1b--table-s3/metadata/00000-2eed5277-661d-47b5-84c0-30ce9dbad149.metadata.json',
      } as const

      jest
        .spyOn(RestCatalogClient.prototype, 'createTable')
        .mockResolvedValue(Promise.resolve(loadTable))

      const response = await app.inject({
        method: 'POST',
        url: `/iceberg/v1/${bucketName}/namespaces/${namespace.name}/tables`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
        payload: {
          name: tableName,
          schema: {
            type: 'struct',
            fields: [
              {
                id: 1,
                name: 'id',
                type: 'long',
                required: false,
              },
              {
                id: 2,
                name: 'name',
                type: 'string',
                required: false,
              },
            ],
            'schema-id': 0,
            'identifier-field-ids': [],
          },
          'partition-spec': {
            'spec-id': 0,
            fields: [],
          },
          'write-order': {
            'order-id': 0,
            fields: [],
          },
          'stage-create': false,
          properties: {},
        },
      })

      expect(response.statusCode).toBe(200)
      const resp = await response.json()
      expect(resp).toEqual(loadTable)
    })

    it('can list tables in a namespace', async () => {
      const bucketName = t.random.name('ice-bucket')
      await t.storage.createIcebergBucket({
        name: bucketName,
        id: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.assignNamespace({
        name: namespaceName,
        bucketId: bucketName,
      })

      const tableName = t.random.name('table')
      await icebergMetastore.createTable({
        name: tableName,
        location: `s3://${bucketName}/tables/${tableName}`,
        namespaceId: namespace.id,
        bucketId: bucketName,
      })

      jest.spyOn(RestCatalogClient.prototype, 'listTables').mockResolvedValue(
        Promise.resolve({
          identifiers: [
            {
              namespace: [namespace.name],
              name: tableName,
            },
          ],
        })
      )

      const response = await app.inject({
        method: 'GET',
        url: `/iceberg/v1/${bucketName}/namespaces/${namespace.name}/tables`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })

      expect(response.statusCode).toBe(200)
      const resp = await response.json()
      expect(resp).toEqual({
        identifiers: [
          {
            namespace: [namespace.name],
            name: tableName,
          },
        ],
      })

      const newTable = await icebergMetastore.findTableByName({
        name: tableName,
      })

      expect(newTable.name).toEqual(tableName)
    })

    it('check if table exists', async () => {
      const bucketName = t.random.name('ice-bucket')
      await t.storage.createIcebergBucket({
        name: bucketName,
        id: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.assignNamespace({
        name: namespaceName,
        bucketId: bucketName,
      })

      const tableName = t.random.name('table')
      await icebergMetastore.createTable({
        name: tableName,
        bucketId: bucketName,
        location: `s3://${bucketName}/tables/${tableName}`,
        namespaceId: namespace.id,
      })

      jest.spyOn(RestCatalogClient.prototype, 'tableExists').mockResolvedValue(Promise.resolve())

      const response = await app.inject({
        method: 'HEAD',
        url: `/iceberg/v1/${bucketName}/namespaces/${namespace.name}/tables/${tableName}`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })

      expect(response.statusCode).toBe(204)
    })

    it('can drop a table', async () => {
      const bucketName = t.random.name('ice-bucket')
      await t.storage.createIcebergBucket({
        name: bucketName,
        id: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.assignNamespace({
        name: namespaceName,
        bucketId: bucketName,
      })

      const tableName = t.random.name('table')
      await icebergMetastore.createTable({
        name: tableName,
        bucketId: bucketName,
        location: `s3://${bucketName}/tables/${tableName}`,
        namespaceId: namespace.id,
      })

      jest.spyOn(RestCatalogClient.prototype, 'dropTable').mockResolvedValue(Promise.resolve())

      const response = await app.inject({
        method: 'DELETE',
        url: `/iceberg/v1/${bucketName}/namespaces/${namespace.name}/tables/${tableName}`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })

      expect(response.statusCode).toBe(204)
      expect(response.body).toEqual('')
    })

    it('can load table metadata', async () => {
      const bucketName = t.random.name('ice-bucket')
      await t.storage.createIcebergBucket({
        name: bucketName,
        id: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.assignNamespace({
        name: namespaceName,
        bucketId: bucketName,
      })

      const tableName = t.random.name('table')
      await icebergMetastore.createTable({
        name: tableName,
        bucketId: bucketName,
        location: `s3://${bucketName}/tables/${tableName}`,
        namespaceId: namespace.id,
      })

      const tableMetadata: LoadTableResult = {
        metadata: {
          'current-schema-id': 0,
          'default-spec-id': 0,
          'format-version': 2,
          'last-column-id': 2,
          'last-updated-ms': 1752062419286,
          location: `s3://${bucketName}/tables/${tableName}`,
          'metadata-log': [],
          'partition-specs': [
            {
              fields: [],
              'spec-id': 0,
            },
          ],
          properties: {
            'write.parquet.compression-codec': 'zstd',
          },
          schemas: [
            {
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
              'schema-id': 0,
              type: 'struct',
            },
          ],
          'table-uuid': 'b734865e-f9a5-4654-8b0b-e15683fcb195',
        },
        'metadata-location': `s3://${bucketName}/tables/${tableName}/metadata/00000-2eed5277-661d-47b5-84c0-30ce9dbad149.metadata.json`,
      }

      jest
        .spyOn(RestCatalogClient.prototype, 'loadTable')
        .mockResolvedValue(Promise.resolve(tableMetadata))

      const response = await app.inject({
        method: 'GET',
        url: `/iceberg/v1/${bucketName}/namespaces/${namespace.name}/tables/${tableName}`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })

      expect(response.statusCode).toBe(200)
      const resp = await response.json()
      expect(resp).toEqual(tableMetadata)

      const table = await icebergMetastore.findTableByName({
        name: tableName,
        tenantId: '',
      })

      expect(table.name).toEqual(tableName)
    })
  })

  describe('S3 Protocol Iceberg Bucket Detection', () => {
    let client: S3Client
    let minioClient: S3Client

    beforeAll(async () => {
      const listener = await app.listen()

      client = new S3Client({
        endpoint: `${listener.replace('[::1]', 'localhost')}/s3`,
        forcePathStyle: true,
        region: storageS3Region,
        credentials: {
          accessKeyId: s3ProtocolAccessKeyId!,
          secretAccessKey: s3ProtocolAccessKeySecret!,
        },
      })

      minioClient = new S3Client({
        endpoint: storageS3Endpoint,
        forcePathStyle: true,
        region: storageS3Region,
      })
    })

    it('will NOT upload the given s3 location if the bucket location is NOT an iceberg table location', async () => {
      const namespace = t.random.name('ice-namespace')
      const tableName = t.random.name('ice-table')
      const internalBucketName = `internal-${Date.now()}--table-s3`

      await createBucketIfNotExists(internalBucketName, minioClient)

      const uploadFile = new PutObjectCommand({
        Bucket: internalBucketName,
        Key: `${namespace}/${tableName}/data.parquet`,
        Body: Buffer.from('test data'),
      })

      try {
        await client.send(uploadFile)
        throw new Error('Expected error not thrown')
      } catch (e) {
        assert(e instanceof Error)
        expect(e.message).not.toContain('Expected error not thrown')

        if (!isS3Error(e)) {
          throw new Error('Expected S3 error but got a different error type')
        }

        expect(e.$metadata.httpStatusCode).toBe(404)
      }
    })

    it('will upload the given s3 location if the bucket location is an iceberg table location', async () => {
      const bucketName = t.random.name('ice-bucket')
      const internalBucketName = `internal-${Date.now()}--table-s3`

      await createBucketIfNotExists(internalBucketName, minioClient)

      await t.storage.createIcebergBucket({
        name: bucketName,
        id: bucketName,
      })

      const tableName = t.random.name('table')
      const namespaceName = t.random.name('namespace')

      const namespace = await icebergMetastore.assignNamespace({
        name: namespaceName,
        bucketId: bucketName,
      })

      await icebergMetastore.createTable({
        name: tableName,
        location: `s3://${internalBucketName}`,
        namespaceId: namespace.id,
        bucketId: bucketName,
      })

      const uploadFile = new PutObjectCommand({
        Bucket: internalBucketName,
        Key: `${namespaceName}/${tableName}/data.parquet`,
        Body: Buffer.from('test data'),
      })

      const response = await client.send(uploadFile)

      expect(response.$metadata.httpStatusCode).toBe(200)
    })
  })
})
