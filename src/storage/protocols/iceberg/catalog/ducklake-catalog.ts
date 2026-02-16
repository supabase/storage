import { Knex } from 'knex'
import {
  GetConfigRequest,
  GetConfigResponse,
  ListNamespacesRequest,
  ListNamespacesResponse,
  CreateNamespaceRequest,
  CreateNamespaceResponse,
  DeleteNamespaceRequest,
  LoadNamespaceMetadataRequest,
  LoadNamespaceMetadataResponse,
  ListTableRequest,
  ListTableResponse,
  LoadTableRequest,
  LoadTableResult,
  CreateTableRequest,
  CreateTableResponse,
  CommitTableRequest,
  DropTableRequest,
  NamespaceExistsRequest,
  TableExistsRequest,
} from './rest-catalog-client'
import { TableIndex } from '../knex'
import { DuckLakeQueries } from './ducklake-queries'
import { buildIcebergSchema } from './ducklake-types'
import { createNoSuchNamespaceError } from './errors'

export interface IcebergCatalogInterface {
  getConfig(params: GetConfigRequest): Promise<any>
  loadNamespaceMetadata(params: LoadNamespaceMetadataRequest): Promise<LoadNamespaceMetadataResponse>
  listNamespaces(params: ListNamespacesRequest): Promise<ListNamespacesResponse>
  listTables(params: ListTableRequest): Promise<ListTableResponse>
  loadTable(params: LoadTableRequest): Promise<LoadTableResult>
  namespaceExists(params: NamespaceExistsRequest): Promise<void>
  tableExists(params: TableExistsRequest): Promise<void>
  findTableByLocation(params: {
    location: string
    tenantId: string
  }): Promise<TableIndex | undefined>

  // Write methods
  createTable(params: CreateTableRequest): Promise<CreateTableResponse>
  updateTable(params: CommitTableRequest): Promise<LoadTableResult>
  dropTable(params: DropTableRequest): Promise<void>
  createNamespace(params: CreateNamespaceRequest): Promise<CreateNamespaceResponse>
  dropNamespace(params: DeleteNamespaceRequest): Promise<void>
}

export interface DuckLakeCatalogOptions {
  db: Knex
  ducklakeSchema: string
  virtualPrefix: string
  dataBucket: string
  warehouseName: string
}

export class DuckLakeCatalog implements IcebergCatalogInterface {
  private queries: DuckLakeQueries
  private virtualPrefix: string
  private dataBucket: string
  private warehouseName: string

  constructor(private readonly options: DuckLakeCatalogOptions) {
    this.queries = new DuckLakeQueries(options.db, options.ducklakeSchema)
    this.virtualPrefix = options.virtualPrefix
    this.dataBucket = options.dataBucket
    this.warehouseName = options.warehouseName
  }

  async getConfig(_params: GetConfigRequest): Promise<GetConfigResponse> {
    return {
      defaults: {
        'write.object-storage.partitioned-paths': 'false',
        's3.delete-enabled': 'false',
        'io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
        'write.object-storage.enabled': 'true',
        prefix: this.warehouseName,
        'rest-metrics-reporting-enabled': 'false',
      } as any,
      overrides: {
        prefix: this.warehouseName,
      },
    }
  }

  async loadNamespaceMetadata(
    params: LoadNamespaceMetadataRequest
  ): Promise<LoadNamespaceMetadataResponse> {
    const schema = await this.queries.findSchemaByName(params.namespace)
    if (!schema) {
      throw createNoSuchNamespaceError(`Namespace '${params.namespace}' not found`)
    }
    return {
      namespace: [schema.schema_name],
      properties: {},
    }
  }

  async listNamespaces(_params: ListNamespacesRequest): Promise<ListNamespacesResponse> {
    const schemas = await this.queries.listSchemas()
    return {
      namespaces: schemas.map((s) => [s.schema_name]),
    }
  }

  async listTables(params: ListTableRequest): Promise<ListTableResponse> {
    const schema = await this.queries.findSchemaByName(params.namespace)
    if (!schema) {
      throw createNoSuchNamespaceError(`Namespace '${params.namespace}' not found`)
    }
    const tables = await this.queries.listTables(schema.schema_id)
    return {
      identifiers: tables.map((t) => ({
        namespace: [params.namespace],
        name: t.table_name,
      })),
    }
  }

  async loadTable(params: LoadTableRequest): Promise<LoadTableResult> {
    const schema = await this.queries.findSchemaByName(params.namespace)
    if (!schema) {
      throw createNoSuchNamespaceError(`Namespace '${params.namespace}' not found`)
    }

    const table = await this.queries.findTableByName(schema.schema_id, params.table)
    if (!table) {
      throw createNoSuchNamespaceError(`Table '${params.table}' not found`)
    }

    const columns = await this.queries.getColumns(table.table_id)
    const icebergSchema = buildIcebergSchema(columns, 0)

    const latestSnapshot = await this.queries.getLatestSnapshot()
    const allSnapshots = await this.queries.getSnapshots()

    const dataPath = await this.queries.getDataPath()
    const tablePathPrefix = await this.queries.getTablePathPrefix(table.table_id)
    const fullPath = `${dataPath}${tablePathPrefix}`
    const tableLocation = fullPath.endsWith('/') ? fullPath.slice(0, -1) : fullPath

    const snapshots = []
    for (const snap of allSnapshots) {
      const dataFiles = await this.queries.getDataFiles(table.table_id, snap.snapshot_id)
      const deleteFiles = await this.queries.getDeleteFiles(table.table_id, snap.snapshot_id)

      const totalRecords = dataFiles.reduce((sum, f) => sum + f.record_count, 0)
      const totalFileSize = dataFiles.reduce((sum, f) => sum + f.file_size_bytes, 0)

      const manifestListPath = `s3://${this.dataBucket}/${this.virtualPrefix}/t${table.table_id}/s${snap.snapshot_id}/snap-${snap.snapshot_id}.avro`

      const parentSnapshotId =
        snap.snapshot_id > 0 ? snap.snapshot_id - 1 : undefined

      snapshots.push({
        'snapshot-id': snap.snapshot_id,
        ...(parentSnapshotId !== undefined
          ? { 'parent-snapshot-id': parentSnapshotId }
          : {}),
        'sequence-number': snap.snapshot_id + 1,
        'timestamp-ms': new Date(snap.snapshot_time).getTime(),
        summary: {
          operation: 'append',
          'spark.app.id': 'ducklake',
          'added-data-files': String(dataFiles.length),
          'added-delete-files': String(deleteFiles.length),
          'added-records': String(totalRecords),
          'added-files-size': String(totalFileSize),
          'total-records': String(totalRecords),
          'total-files-size': String(totalFileSize),
          'total-data-files': String(dataFiles.length),
          'total-delete-files': String(deleteFiles.length),
          'total-equality-deletes': '0',
          'total-position-deletes': String(deleteFiles.reduce((s, f) => s + f.delete_count, 0)),
        },
        'manifest-list': manifestListPath,
        'schema-id': 0,
      })
    }

    const currentSnapshotId = latestSnapshot?.snapshot_id ?? -1
    const lastColumnId = columns.length > 0 ? Math.max(...columns.map((c) => c.column_id)) : 0

    const metadata: any = {
      'format-version': 2,
      'table-uuid': `ducklake-t${table.table_id}`,
      location: tableLocation,
      'last-updated-ms': latestSnapshot
        ? new Date(latestSnapshot.snapshot_time).getTime()
        : Date.now(),
      properties: {},
      schemas: [icebergSchema],
      'current-schema-id': 0,
      'last-column-id': lastColumnId,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'default-spec-id': 0,
      'last-partition-id': 999,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
      snapshots: snapshots,
      'current-snapshot-id': currentSnapshotId >= 0 ? currentSnapshotId : -1,
      refs:
        currentSnapshotId >= 0
          ? {
              main: {
                'snapshot-id': currentSnapshotId,
                type: 'branch',
              },
            }
          : {},
      'snapshot-log': snapshots.map((s) => ({
        'timestamp-ms': s['timestamp-ms'],
        'snapshot-id': s['snapshot-id'],
      })),
      'metadata-log': [],
    }

    return {
      'metadata-location': `s3://${this.dataBucket}/${this.virtualPrefix}/t${table.table_id}/metadata.json`,
      metadata,
    }
  }

  async namespaceExists(params: NamespaceExistsRequest): Promise<void> {
    const schema = await this.queries.findSchemaByName(params.namespace)
    if (!schema) {
      throw createNoSuchNamespaceError(`Namespace '${params.namespace}' not found`)
    }
  }

  async tableExists(params: TableExistsRequest): Promise<void> {
    const schema = await this.queries.findSchemaByName(params.namespace)
    if (!schema) {
      throw createNoSuchNamespaceError(`Namespace '${params.namespace}' not found`)
    }
    const table = await this.queries.findTableByName(schema.schema_id, params.table)
    if (!table) {
      throw createNoSuchNamespaceError(`Table '${params.table}' not found`)
    }
  }

  async findTableByLocation(params: {
    location: string
    tenantId: string
  }): Promise<TableIndex | undefined> {
    // In DuckLake mode, we match based on the data bucket
    const bucketUrl = `s3://${this.dataBucket}`
    if (params.location.startsWith(bucketUrl)) {
      return {
        id: 'ducklake',
        name: 'ducklake',
        catalog_id: 'ducklake',
        bucket_name: this.dataBucket,
        namespace_id: 'ducklake',
        location: bucketUrl,
      }
    }
    return undefined
  }

  // Write methods - DuckLake catalog is read-only
  async createTable(_params: CreateTableRequest): Promise<CreateTableResponse> {
    throw new Error('DuckLake catalog is read-only')
  }

  async updateTable(_params: CommitTableRequest): Promise<LoadTableResult> {
    throw new Error('DuckLake catalog is read-only')
  }

  async dropTable(_params: DropTableRequest): Promise<void> {
    throw new Error('DuckLake catalog is read-only')
  }

  async createNamespace(_params: CreateNamespaceRequest): Promise<CreateNamespaceResponse> {
    throw new Error('DuckLake catalog is read-only')
  }

  async dropNamespace(_params: DeleteNamespaceRequest): Promise<void> {
    throw new Error('DuckLake catalog is read-only')
  }
}
