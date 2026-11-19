import assert from 'node:assert'
import { randomUUID } from 'node:crypto'
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { signJWT } from '@internal/auth'
import { isS3Error } from '@internal/errors'
import { DeleteIcebergResourcesHandler, QUEUES } from '@storage/events'
import * as eventsV2Base from '@storage/events/base'
import {
  CreateTableResponse,
  LoadTableResult,
  RestCatalogClient,
} from '@storage/protocols/iceberg/catalog'
import { Metastore } from '@storage/protocols/iceberg/metastore'
import { PgMetastore } from '@storage/protocols/iceberg/pg'
import { FastifyInstance } from 'fastify'
import { Client } from 'pg'
import { fetch as undiciFetch } from 'undici'
import { getConfig, mergeConfig } from '../config'
import { createSignedS3Request } from './utils/signature-v4'
import { createBucketIfNotExists, useStorage } from './utils/storage'

const {
  jwtSecret,
  serviceKeyAsync,
  storageS3Region,
  storageS3Endpoint,
  s3ProtocolAccessKeyId,
  s3ProtocolAccessKeySecret,
  tenantId,
} = getConfig()

const icebergLimitOverrides = {
  icebergMaxCatalogsCount: 1e8,
  icebergMaxNamespaceCount: 1e8,
}

describe('Iceberg Catalog', () => {
  const t = useStorage()

  let app: FastifyInstance
  let icebergMetastore: Metastore
  beforeAll(async () => {
    mergeConfig(icebergLimitOverrides)

    const { default: makeApp } = await import('../app')
    app = makeApp()
    icebergMetastore = new PgMetastore(t.database.connection, {
      multiTenant: false,
      schema: 'storage',
    })
  })

  afterAll(async () => {
    await app.close()
    t.database.connection.dispose()
  })

  it('can create an analytic bucket', async () => {
    const bucketName = t.random.name('ice-bucket')

    const response = await app.inject({
      method: 'POST',
      url: '/iceberg/bucket',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${await serviceKeyAsync}`,
      },
      payload: {
        name: bucketName,
      },
    })

    const resp = await response.json()
    expect(response.statusCode).toBe(200)
    expect(resp.id).toBe(bucketName)
  })

  it('rejects an analytics bucket created with a non-DISABLED versioning_status', async () => {
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
        versioning_status: 'ENABLED',
      },
    })

    expect(response.statusCode).toBe(400)

    const listResponse = await app.inject({
      method: 'GET',
      url: '/iceberg/bucket',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })
    const list: Array<{ id: string }> = await listResponse.json()
    expect(list.some((b) => b.id === bucketName)).toBe(false)
  })

  it('can list analytic buckets', async () => {
    const bucketName = t.random.name('ice-bucket')
    await t.storage.createIcebergBucket({
      name: bucketName,
    })

    const response = await app.inject({
      method: 'GET',
      url: '/iceberg/bucket',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })

    const resp = await response.json()
    expect(response.statusCode).toBe(200)
    expect(resp.length).toBeGreaterThan(0)
  })

  it('can delete analytic bucket', async () => {
    const bucketName = t.random.name('ice-bucket')
    const bucket = await t.storage.createIcebergBucket({
      name: bucketName,
    })

    const response = await app.inject({
      method: 'DELETE',
      url: `/iceberg/bucket/${bucketName}`,
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })

    expect(response.statusCode).toBe(200)

    const deletedCatalog = await t.database.connection.query<{
      deleted_at: Date | string | null
    }>({
      text: `
        SELECT deleted_at
        FROM storage.buckets_analytics
        WHERE id = $1
        LIMIT 1
      `,
      values: [bucket.id],
    })

    expect(deletedCatalog.rows[0]?.deleted_at).toBeTruthy()
  })

  it('can empty analytic bucket resources after deletion starts', async () => {
    const bucketName = t.random.name('ice-bucket')
    const bucket = await t.storage.createIcebergBucket({
      name: bucketName,
    })

    const namespaceName = t.random.name('namespace')
    const namespace = await icebergMetastore.createNamespace({
      name: namespaceName,
      bucketName: bucket.name,
      bucketId: bucket.id,
      tenantId: '',
      metadata: {},
    })

    const tableName = t.random.name('table')
    await icebergMetastore.createTable({
      name: tableName,
      bucketName: bucket.name,
      bucketId: bucket.id,
      location: `s3://${bucketName}/tables/${tableName}`,
      namespaceId: namespace.id,
      shardId: 'my-warehouse',
      shardKey: 'my-warehouse',
    })

    await t.database.deleteAnalyticsBucket(bucket.id, { soft: true })

    const dropTable = vi.spyOn(RestCatalogClient.prototype, 'dropTable').mockResolvedValue()
    const listNamespaces = vi
      .spyOn(RestCatalogClient.prototype, 'listNamespaces')
      .mockResolvedValue({ namespaces: [] })
    const listTables = vi.spyOn(RestCatalogClient.prototype, 'listTables').mockResolvedValue({
      identifiers: [],
    })
    const dropNamespace = vi.spyOn(RestCatalogClient.prototype, 'dropNamespace').mockResolvedValue()
    vi.spyOn(
      eventsV2Base as unknown as {
        createStorage(payload: unknown): Promise<typeof t.storage>
      },
      'createStorage'
    ).mockResolvedValue({
      ...t.storage,
      db: {
        ...t.storage.db,
        connection: t.storage.db.connection,
        destroyConnection: vi.fn(),
      },
    } as unknown as typeof t.storage)

    await new DeleteIcebergResourcesHandler().handle({
      queue: QUEUES.deleteIcebergResources,
      message: {
        id: 'test-delete-iceberg-resources',
        data: {
          catalogId: bucket.id,
          tenant: {
            ref: '',
            host: 'localhost',
          },
        },
        headers: {},
        timestamp: Date.now(),
        attempt: 1,
      },
      signal: new AbortController().signal,
      heartbeat: async () => {},
    } as never)

    const remoteNamespace = `_${namespace.id.replaceAll('-', '_')}`
    expect(listNamespaces).toHaveBeenCalledWith({ warehouse: 'my-warehouse', pageSize: 1 })
    expect(dropTable).toHaveBeenCalledWith({
      namespace: remoteNamespace,
      table: tableName,
      purgeRequested: true,
      warehouse: 'my-warehouse',
    })
    expect(listTables).toHaveBeenCalledWith({
      namespace: remoteNamespace,
      pageSize: 1000,
      warehouse: 'my-warehouse',
    })
    expect(dropNamespace).toHaveBeenCalledWith({
      namespace: remoteNamespace,
      warehouse: 'my-warehouse',
    })

    await expect(
      icebergMetastore.findCatalogById({
        id: bucket.id,
        tenantId: '',
        deleted: true,
      })
    ).rejects.toThrow()

    await expect(
      icebergMetastore.findNamespaceByName({
        name: namespace.name,
        catalogId: bucket.id,
        tenantId: '',
      })
    ).rejects.toThrow()
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
    })

    vi.spyOn(RestCatalogClient.prototype, 'getConfig').mockResolvedValue({
      defaults: {
        prefix: bucketName,
      },
      overrides: {
        prefix: bucketName,
      },
    })

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
      'io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
      prefix: bucketName,
      'rest-metrics-reporting-enabled': 'false',
      's3.delete-enabled': 'false',
      'write.object-storage.enabled': 'true',
      'write.object-storage.partitioned-paths': 'false',
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
      })

      const namespaceName = t.random.name('namespace')

      vi.spyOn(RestCatalogClient.prototype, 'createNamespace').mockResolvedValue({
        namespace: [namespaceName],
        properties: {
          test: 'hello',
        },
      })

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

      expect(response.statusCode, response.body).toBe(200)
      const resp = await response.json()
      expect(resp).toEqual({
        namespace: [namespaceName],
        properties: {
          test: 'hello',
        },
      })
    })

    it('updates namespace properties when creating an existing namespace', async () => {
      const bucketName = t.random.name('ice-bucket')
      await t.storage.createIcebergBucket({
        name: bucketName,
      })

      const namespaceName = t.random.name('namespace')

      const createResponse = await app.inject({
        method: 'POST',
        url: `/iceberg/v1/${bucketName}/namespaces`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
        payload: {
          namespace: namespaceName,
          properties: {
            stable: 'kept',
            overwrite: 'old',
          },
        },
      })

      expect(createResponse.statusCode, createResponse.body).toBe(200)
      expect(await createResponse.json()).toEqual({
        namespace: [namespaceName],
        properties: {
          stable: 'kept',
          overwrite: 'old',
        },
      })

      const updateResponse = await app.inject({
        method: 'POST',
        url: `/iceberg/v1/${bucketName}/namespaces`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
        payload: {
          namespace: namespaceName,
          properties: {
            overwrite: 'new',
            added: 'value',
          },
        },
      })

      expect(updateResponse.statusCode).toBe(200)
      expect(await updateResponse.json()).toEqual({
        namespace: [namespaceName],
        properties: {
          stable: 'kept',
          overwrite: 'new',
          added: 'value',
        },
      })

      const loadResponse = await app.inject({
        method: 'GET',
        url: `/iceberg/v1/${bucketName}/namespaces/${namespaceName}`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })

      expect(loadResponse.statusCode).toBe(200)
      expect(await loadResponse.json()).toEqual({
        namespace: [namespaceName],
        properties: {
          stable: 'kept',
          overwrite: 'new',
          added: 'value',
        },
      })
    })

    it('returns InvalidParameter for invalid namespace names', async () => {
      const bucketName = t.random.name('ice-bucket')
      await t.storage.createIcebergBucket({
        name: bucketName,
      })

      const response = await app.inject({
        method: 'POST',
        url: `/iceberg/v1/${bucketName}/namespaces`,
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${await serviceKeyAsync}`,
        },
        payload: {
          namespace: 'awsnamespace',
        },
      })

      expect(response.statusCode).toBe(400)
      expect(await response.json()).toEqual({
        error: {
          code: 400,
          message: 'Resource name must not start with the reserved prefix "aws"',
          type: 'InvalidParameter',
        },
      })
    })

    it('can list namespaces', async () => {
      const bucketName = t.random.name('ice-bucket')

      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
      })

      const namespaceName = t.random.name('namespace')

      const namespace = await icebergMetastore.createNamespace({
        name: namespaceName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {},
      })

      vi.spyOn(RestCatalogClient.prototype, 'listNamespaces').mockResolvedValue({
        namespaces: [[namespaceName]],
      })

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
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.createNamespace({
        name: namespaceName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {},
      })

      const initialNamespaces = await icebergMetastore.listNamespaces({
        catalogId: bucket.id,
      })

      vi.spyOn(RestCatalogClient.prototype, 'dropNamespace').mockResolvedValue(undefined)

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
        catalogId: bucket.id,
      })

      expect(afterDropNamespaces.length).toEqual(initialNamespaces.length - 1)
    })

    it('can load namespace metadata', async () => {
      const bucketName = t.random.name('ice-bucket')

      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.createNamespace({
        name: namespaceName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {
          test: 'hello',
        },
      })

      vi.spyOn(RestCatalogClient.prototype, 'loadNamespaceMetadata').mockResolvedValue({
        namespace: [namespace.name],
        properties: {
          test: 'hello',
        },
      })

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
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.createNamespace({
        name: namespaceName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {},
      })

      vi.spyOn(RestCatalogClient.prototype, 'namespaceExists').mockResolvedValue(undefined)

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
      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.createNamespace({
        name: namespaceName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {},
      })

      const tableName = t.random.name('table')
      const tableLocation = `s3://${bucketName}/tables/${tableName}`
      const tableMetadataLocation = `${tableLocation}/metadata/00000-2eed5277-661d-47b5-84c0-30ce9dbad149.metadata.json`
      const loadTable: CreateTableResponse = {
        metadata: {
          'current-schema-id': 0,
          'default-spec-id': 0,
          'format-version': 2,
          'last-column-id': 2,
          'last-updated-ms': 1752062419286,
          location: tableLocation,
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
        'metadata-location': tableMetadataLocation,
      } as const

      vi.spyOn(RestCatalogClient.prototype, 'createTable').mockResolvedValue(loadTable)

      vi.spyOn(RestCatalogClient.prototype, 'createNamespace').mockResolvedValue({
        namespace: [namespace.name],
        properties: {},
      })

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
      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.createNamespace({
        name: namespaceName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {},
      })

      const tableName = t.random.name('table')
      await icebergMetastore.createTable({
        name: tableName,
        location: `s3://${bucketName}/tables/${tableName}`,
        namespaceId: namespace.id,
        bucketName: bucket.name,
        bucketId: bucket.id,
      })

      vi.spyOn(RestCatalogClient.prototype, 'listTables').mockResolvedValue({
        identifiers: [
          {
            namespace: [namespace.name],
            name: tableName,
          },
        ],
      })

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
        namespaceId: namespace.id,
      })

      expect(newTable.name).toEqual(tableName)
    })

    it('check if table exists', async () => {
      const bucketName = t.random.name('ice-bucket')
      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.createNamespace({
        name: namespaceName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {},
      })

      const tableName = t.random.name('table')
      await icebergMetastore.createTable({
        name: tableName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        location: `s3://${bucketName}/tables/${tableName}`,
        namespaceId: namespace.id,
        shardId: 'my-warehouse',
        shardKey: 'my-warehouse',
      })

      vi.spyOn(RestCatalogClient.prototype, 'tableExists').mockResolvedValue(undefined)

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
      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.createNamespace({
        name: namespaceName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {},
      })

      const tableName = t.random.name('table')
      await icebergMetastore.createTable({
        name: tableName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        location: `s3://${bucketName}/tables/${tableName}`,
        namespaceId: namespace.id,
        shardId: 'my-warehouse',
        shardKey: 'my-warehouse',
      })

      vi.spyOn(RestCatalogClient.prototype, 'dropTable').mockResolvedValue(undefined)
      vi.spyOn(RestCatalogClient.prototype, 'listTables').mockResolvedValue({ identifiers: [] })

      vi.spyOn(RestCatalogClient.prototype, 'dropNamespace').mockResolvedValue(undefined)

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
      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
      })

      const namespaceName = t.random.name('namespace')
      const namespace = await icebergMetastore.createNamespace({
        name: namespaceName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {},
      })

      const tableName = t.random.name('table')
      await icebergMetastore.createTable({
        name: tableName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        location: `s3://${bucketName}/tables/${tableName}`,
        namespaceId: namespace.id,
        shardId: 'my-warehouse',
        shardKey: 'my-warehouse',
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

      vi.spyOn(RestCatalogClient.prototype, 'loadTable').mockResolvedValue(tableMetadata)

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
        namespaceId: namespace.id,
      })

      expect(table.name).toEqual(tableName)
    })
  })

  describe('S3 Protocol Iceberg Bucket Detection', () => {
    let client: S3Client
    let rustfsClient: S3Client
    let endpoint: string
    let baseUrl: string

    beforeAll(async () => {
      const listener = await app.listen()
      baseUrl = listener.replace('[::1]', 'localhost')
      endpoint = `${baseUrl}/s3`

      client = new S3Client({
        endpoint,
        forcePathStyle: true,
        region: storageS3Region,
        credentials: {
          accessKeyId: s3ProtocolAccessKeyId!,
          secretAccessKey: s3ProtocolAccessKeySecret!,
        },
      })

      rustfsClient = new S3Client({
        endpoint: storageS3Endpoint,
        forcePathStyle: true,
        region: storageS3Region,
      })
    })

    async function createIcebergTableBucket() {
      // S3 bucket names can't contain underscores, so this can't use t.random.name() directly.
      const internalBucketName = `internal-${Date.now()}-${Math.random().toString(36).slice(2, 10)}--table-s3`
      await createBucketIfNotExists(internalBucketName, rustfsClient)

      const bucket = await t.storage.createIcebergBucket({
        name: t.random.name('ice-bucket'),
      })

      const tableName = t.random.name('table')
      const namespaceName = t.random.name('namespace')

      const namespace = await icebergMetastore.createNamespace({
        name: namespaceName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {},
      })

      await icebergMetastore.createTable({
        name: tableName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        location: `s3://${internalBucketName}`,
        namespaceId: namespace.id,
      })

      return { internalBucketName, key: `${namespaceName}/${tableName}/data.parquet` }
    }

    it('will NOT upload the given s3 location if the bucket location is NOT an iceberg table location', async () => {
      const namespace = t.random.name('ice-namespace')
      const tableName = t.random.name('ice-table')
      const internalBucketName = `internal-${randomUUID()}--table-s3`

      await createBucketIfNotExists(internalBucketName, rustfsClient)

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
      const internalBucketName = `internal-${randomUUID()}--table-s3`

      await createBucketIfNotExists(internalBucketName, rustfsClient)

      const bucket = await t.storage.createIcebergBucket({
        name: bucketName,
      })

      const tableName = t.random.name('table')
      const namespaceName = t.random.name('namespace')

      const namespace = await icebergMetastore.createNamespace({
        name: namespaceName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {},
      })

      await icebergMetastore.createTable({
        name: tableName,
        bucketName: bucket.name,
        bucketId: bucket.id,
        location: `s3://${internalBucketName}`,
        namespaceId: namespace.id,
      })

      const uploadFile = new PutObjectCommand({
        Bucket: internalBucketName,
        Key: `${namespaceName}/${tableName}/data.parquet`,
        Body: Buffer.from('test data'),
      })

      const response = await client.send(uploadFile)

      expect(response.$metadata.httpStatusCode).toBe(200)
    })

    it('denies session credentials without the service role on an iceberg table location', async () => {
      const internalBucketName = `internal-${randomUUID()}--table-s3`
      await createBucketIfNotExists(internalBucketName, rustfsClient)

      const bucket = await t.storage.createIcebergBucket({ name: t.random.name('ice-bucket') })
      const namespace = await icebergMetastore.createNamespace({
        name: t.random.name('namespace'),
        bucketName: bucket.name,
        bucketId: bucket.id,
        tenantId: '',
        metadata: {},
      })
      await icebergMetastore.createTable({
        name: t.random.name('table'),
        bucketName: bucket.name,
        bucketId: bucket.id,
        location: `s3://${internalBucketName}`,
        namespaceId: namespace.id,
      })
      await rustfsClient.send(
        new PutObjectCommand({ Bucket: internalBucketName, Key: 'secret', Body: 'private bytes' })
      )

      const anon = new S3Client({
        endpoint,
        forcePathStyle: true,
        region: storageS3Region,
        credentials: {
          accessKeyId: tenantId,
          secretAccessKey: tenantId,
          sessionToken: await signJWT({ role: 'anon' }, jwtSecret, '5m'),
        },
      })

      await expect(
        anon.send(new GetObjectCommand({ Bucket: internalBucketName, Key: 'secret' }))
      ).rejects.toMatchObject({ name: 'AccessDenied', $metadata: { httpStatusCode: 403 } })
      await expect(
        anon.send(
          new PutObjectCommand({ Bucket: internalBucketName, Key: 'secret', Body: 'overwritten' })
        )
      ).rejects.toMatchObject({ name: 'AccessDenied', $metadata: { httpStatusCode: 403 } })

      const object = await rustfsClient.send(
        new GetObjectCommand({ Bucket: internalBucketName, Key: 'secret' })
      )
      expect(await object.Body?.transformToString()).toBe('private bytes')
    })

    it('allows an Iceberg PutObject request carrying an unsigned content-type header', async () => {
      // Real clients (e.g. the unmaintained aws-sdk-js v2) never sign
      // content-type on uploads; requiring it would break otherwise-legitimate uploads.
      const { internalBucketName, key } = await createIcebergTableBucket()
      const body = 'test data'

      const signedRequest = await createSignedS3Request({
        baseUrl,
        path: `/s3/${internalBucketName}/${key}`,
        method: 'PUT',
        body,
        includeContentLength: true,
      })

      const resp = await undiciFetch(signedRequest.requestUrl, {
        method: 'PUT',
        headers: {
          ...signedRequest.headers,
          'content-type': 'text/html',
        },
        body,
      })

      expect(resp.status).toBe(200)
    })

    it('allows an Iceberg PutObject request carrying an unsigned content-encoding header', async () => {
      const { internalBucketName, key } = await createIcebergTableBucket()
      const body = 'test data'

      const signedRequest = await createSignedS3Request({
        baseUrl,
        path: `/s3/${internalBucketName}/${key}`,
        method: 'PUT',
        body,
        includeContentLength: true,
      })

      const resp = await undiciFetch(signedRequest.requestUrl, {
        method: 'PUT',
        headers: {
          ...signedRequest.headers,
          'content-encoding': 'gzip, aws-chunked',
        },
        body,
      })

      expect(resp.status).toBe(200)
    })

    it('allows an Iceberg CreateMultipartUpload request carrying an unsigned content-type header', async () => {
      const { internalBucketName, key } = await createIcebergTableBucket()

      const signedRequest = await createSignedS3Request({
        baseUrl,
        path: `/s3/${internalBucketName}/${key}`,
        method: 'POST',
        query: { uploads: '' },
        body: '',
        includeContentLength: true,
      })

      const resp = await undiciFetch(signedRequest.requestUrl, {
        method: 'POST',
        headers: {
          ...signedRequest.headers,
          'content-type': 'application/json',
        },
        body: '',
      })

      expect(resp.status).toBe(200)
    })

    it('allows an Iceberg CreateMultipartUpload request carrying an unsigned content-encoding header', async () => {
      const { internalBucketName, key } = await createIcebergTableBucket()

      const signedRequest = await createSignedS3Request({
        baseUrl,
        path: `/s3/${internalBucketName}/${key}`,
        method: 'POST',
        query: { uploads: '' },
        body: '',
        includeContentLength: true,
      })

      const resp = await undiciFetch(signedRequest.requestUrl, {
        method: 'POST',
        headers: {
          ...signedRequest.headers,
          'content-encoding': 'gzip, aws-chunked',
        },
        body: '',
      })

      expect(resp.status).toBe(200)
    })

    it('rejects an Iceberg UploadPart request carrying an unsigned x-amz-decoded-content-length header', async () => {
      const { internalBucketName, key } = await createIcebergTableBucket()
      const body = 'part-body'

      const signedRequest = await createSignedS3Request({
        baseUrl,
        path: `/s3/${internalBucketName}/${key}`,
        method: 'PUT',
        query: { partNumber: '1', uploadId: 'nonexistent-upload-id' },
        body,
        includeContentLength: true,
      })

      const resp = await undiciFetch(signedRequest.requestUrl, {
        method: 'PUT',
        headers: {
          ...signedRequest.headers,
          'x-amz-decoded-content-length': '999999',
        },
        body,
      })

      expect(resp.status).toBe(403)
      expect(await resp.text()).toContain('AccessDenied')
    })
  })
})

describe('Iceberg namespace metastore', () => {
  const params = {
    name: 'namespace',
    bucketId: 'catalog-id',
    bucketName: 'catalog',
    tenantId: 'tenant-id',
    metadata: { changed: 'yes' },
  }

  async function withNamespace(
    multiTenant: boolean,
    test: (client: Client, schema: string) => Promise<void>
  ) {
    const client = new Client({ connectionString: getConfig().databaseURL })
    const schema = `namespace_test_${randomUUID().replaceAll('-', '')}`
    await client.connect()
    try {
      await client.query('BEGIN')
      await client.query(`CREATE SCHEMA "${schema}"`)
      await client.query(`
        CREATE TABLE "${schema}".iceberg_namespaces (
          id text DEFAULT 'namespace-id', name text NOT NULL, catalog_id text NOT NULL,
          bucket_name text NOT NULL, tenant_id text NOT NULL DEFAULT 'tenant-id',
          metadata jsonb NOT NULL, updated_at timestamptz DEFAULT now(),
          UNIQUE (${multiTenant ? 'tenant_id, ' : ''}catalog_id, name)
        )
      `)
      await client.query(`
        INSERT INTO "${schema}".iceberg_namespaces
          (name, catalog_id, bucket_name, tenant_id, metadata)
        VALUES ('namespace', 'catalog-id', 'catalog', 'tenant-id', '{"original":"yes"}')
      `)
      await test(client, schema)
    } finally {
      await client.query('ROLLBACK')
      await client.end()
    }
  }

  it.each([
    [false, 'INSERT'],
    [true, 'INSERT'],
    [false, 'UPDATE'],
    [true, 'UPDATE'],
  ] as const)(
    'respects a BEFORE trigger that skips a write (multitenant=%s, event=%s)',
    async (multiTenant, event) => {
      await withNamespace(multiTenant, async (client, schema) => {
        await client.query(`CREATE SEQUENCE "${schema}".trigger_calls`)
        await client.query(`
          CREATE FUNCTION "${schema}".skip_first_write() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN
            IF nextval('"${schema}".trigger_calls') = 1 THEN RETURN NULL; END IF;
            RETURN NEW;
          END $$
        `)
        await client.query(`
          CREATE TRIGGER skip_first_write BEFORE ${event} ON "${schema}".iceberg_namespaces
          FOR EACH ROW EXECUTE FUNCTION "${schema}".skip_first_write()
        `)
        const metastore = new PgMetastore(client as never, { schema, multiTenant })
        await expect(metastore.createNamespace(params)).rejects.toMatchObject({ code: 'NoSuchKey' })
        const result = await client.query(`SELECT metadata FROM "${schema}".iceberg_namespaces`)
        expect(result.rows[0].metadata).toEqual({ original: 'yes' })
      })
    }
  )

  it.each([false, true])(
    'updates an existing namespace after a foreign-key lock (multitenant=%s)',
    async (multiTenant) => {
      const client = new Client({ connectionString: getConfig().databaseURL })
      const schema = `namespace_checkpoint_${randomUUID().replaceAll('-', '')}`
      await client.connect()
      try {
        await client.query(`CREATE SCHEMA "${schema}"`)
        await client.query(`
          CREATE TABLE "${schema}".iceberg_namespaces (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            name text NOT NULL, catalog_id text NOT NULL, bucket_name text NOT NULL,
            tenant_id text NOT NULL DEFAULT 'tenant-id', metadata jsonb NOT NULL,
            updated_at timestamptz DEFAULT now(),
            UNIQUE (${multiTenant ? 'tenant_id, ' : ''}catalog_id, name)
          )
        `)
        await client.query(`
          CREATE TABLE "${schema}".namespace_child (
            id int PRIMARY KEY, namespace_id uuid REFERENCES "${schema}".iceberg_namespaces(id)
          )
        `)
        const metastore = new PgMetastore(client as never, { schema, multiTenant })
        const first = await metastore.createNamespace({
          ...params,
          metadata: { stable: 'kept', overwrite: 'old' },
        })
        await client.query(`INSERT INTO "${schema}".namespace_child VALUES (1, $1)`, [first.id])
        const method = await client.query<{ amname: string }>(
          `SELECT am.amname FROM pg_class c JOIN pg_am am ON am.oid = c.relam
           WHERE c.oid = $1::regclass`,
          [`"${schema}".iceberg_namespaces`]
        )
        // A checkpoint moves the committed foreign-key lock past the retained
        // undo boundary, which exposed the upstream OrioleDB regression.
        if (method.rows[0]?.amname === 'orioledb') await client.query('CHECKPOINT')

        await expect(
          metastore.createNamespace({ ...params, metadata: { overwrite: 'new' } })
        ).resolves.toMatchObject({ id: first.id, metadata: { stable: 'kept', overwrite: 'new' } })
        const stored = await client.query(
          `SELECT metadata FROM "${schema}".iceberg_namespaces WHERE id = $1`,
          [first.id]
        )
        expect(stored.rows[0].metadata).toEqual({ stable: 'kept', overwrite: 'new' })
      } finally {
        await client.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
        await client.end()
      }
    },
    60_000
  )
})
