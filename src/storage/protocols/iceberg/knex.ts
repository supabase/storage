import { Knex } from 'knex'
import { ERRORS } from '@internal/errors'
import { IcebergCatalog } from '@storage/schemas'
import { DBError } from '@storage/database'
import { hashStringToInt } from '@internal/hashing'

export interface CreateNamespaceParams {
  name: string
  bucketName: string
  bucketId: string
  tenantId?: string
  metadata: Record<string, string | number>
}

export interface ListNamespaceParams {
  tenantId?: string
  catalogId: string
}

export interface NamespaceIndex {
  id: string
  name: string
  catalog_id: string
  bucket_name: string
  tenant_id?: string
  metadata?: Record<string, string | number>
}

export interface Catalog {
  id: string
  name: string
  tenant_id?: string
}

export interface TableIndex {
  id: string
  name: string
  catalog_id: string
  bucket_name: string
  namespace_id: string
  location: string
  tenant_id?: string
  shard_key?: string
  shard_id?: string
  remote_table_id?: string
}

export interface DropNamespaceParams {
  namespace: string
  catalogId: string
  tenantId?: string
}

export interface CreateTableParams {
  name: string
  bucketId: string
  bucketName: string
  namespaceId: string
  tenantId?: string
  shardKey?: string
  shardId?: string
  location: string
  remoteTableId?: string
}

export interface Metastore<Tnx = unknown> {
  createNamespace(params: CreateNamespaceParams): Promise<NamespaceIndex>
  listNamespaces(params: ListNamespaceParams): Promise<NamespaceIndex[]>
  dropNamespace(params: DropNamespaceParams): Promise<void>
  dropCatalog(params: { tenantId?: string; bucketId: string }): Promise<boolean>

  createTable(params: CreateTableParams): Promise<TableIndex>
  dropTable(params: {
    name: string
    namespaceId: string
    catalogId: string
    tenantId: string
  }): Promise<void>
  findTableByLocation(params: { tenantId?: string; location: string }): Promise<TableIndex>
  findTableById(params: { tenantId?: string; namespaceId: string; id: string }): Promise<TableIndex>
  findTableByName(params: {
    tenantId?: string
    name: string
    namespaceId: string
  }): Promise<TableIndex>
  findNamespaceByName(params: {
    tenantId: string
    catalogId: string
    name: string
  }): Promise<NamespaceIndex>
  transaction<T>(
    callback: (trx: KnexMetastore) => Promise<T>,
    opts?: { isolationLevel?: Knex.IsolationLevels }
  ): Promise<T>

  assignCatalog(param: { bucketName: string; tenantId: string }): Promise<any>
  countCatalogs(params: { tenantId: string; limit: number }): Promise<number>
  countNamespaces(param: { tenantId: string; limit: number }): Promise<number>
  countTables(params: { namespaceId: string; tenantId?: string; limit: number }): Promise<number>
  countResources(params: {
    tenantId?: string
    limit: number
  }): Promise<{ namespaces: number; tables: number }>
  findCatalogByName(param: {
    tenantId: string
    name: string
    deleted?: boolean
  }): Promise<IcebergCatalog>

  findCatalogById(param: {
    tenantId: string
    id: string
    deleted?: boolean
  }): Promise<IcebergCatalog>

  listTables(param: {
    tenantId: string
    pageSize: number | undefined
    namespaceId: string
  }): Promise<TableIndex[]>

  lockResource(resourceType: string, resourceId: string): Promise<void>

  getTnx(): Tnx
}

export class KnexMetastore implements Metastore<Knex.Transaction> {
  constructor(
    private readonly db: Knex | Knex.Transaction,
    private readonly ops: { schema: string; multiTenant?: boolean }
  ) {}

  lockResource(resourceType: string, resourceId: string): Promise<void> {
    const lockId = hashStringToInt(`${resourceType}:${resourceId}`)
    return this.db.raw('SELECT pg_advisory_xact_lock(?::bigint)', [lockId]).then(() => {})
  }

  getTnx() {
    if (this.db.isTransaction) {
      return this.db as Knex.Transaction
    }

    throw new Error('Not in a transaction')
  }

  async dropCatalog(params: {
    tenantId?: string | undefined
    bucketId: string
    soft?: boolean
  }): Promise<boolean> {
    const table = this.ops.multiTenant ? 'iceberg_catalogs' : 'buckets_analytics'

    if (params.soft) {
      const query = this.db
        .withSchema(this.ops.schema)
        .table(table)
        .andWhere('id', params.bucketId)
        .update({
          deleted_at: new Date(),
        })

      if (this.ops.multiTenant) {
        query.andWhere('tenant_id', params.tenantId)
      }

      const n = await query

      return n > 0
    }

    const query = this.db
      .withSchema(this.ops.schema)
      .table(table)
      .andWhere('id', params.bucketId)
      .del()

    if (this.ops.multiTenant) {
      query.andWhere('tenant_id', params.tenantId)
    }

    const del = await query

    return del > 0
  }

  listTables(param: {
    tenantId: string
    pageSize: number | undefined
    namespaceId: string
  }): Promise<TableIndex[]> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .select<TableIndex[]>('id', 'name', 'namespace_id', 'shard_id', 'shard_key')
      .where('namespace_id', param.namespaceId)

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', param.tenantId)
    }

    if (param.pageSize) {
      query.limit(param.pageSize)
    }

    return query.orderBy('created_at', 'asc')
  }

  async countResources(params: {
    bucketId: string
    tenantId?: string | undefined
    limit: number
  }): Promise<{ namespaces: number; tables: number }> {
    const countNamespaces = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_namespaces')
      .where('bucket_name', params.bucketId)
      .limit(params.limit)
      .count<{ count: string }>('id as n_count')

    if (this.ops.multiTenant) {
      countNamespaces.andWhere('tenant_id', params.tenantId)
    }

    const countTables = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .where('bucket_name', params.bucketId)
      .limit(params.limit)
      .count<{ count: string }>('id as t_count')

    if (this.ops.multiTenant) {
      countTables.andWhere('tenant_id', params.tenantId)
    }

    const resultQuery = this.db
      .with('namespace_count', countNamespaces)
      .with('table_count', countTables)
      .select<{ count: string }[]>(this.db.raw('namespace_count.n_count, table_count.t_count'))
      .from(this.db.raw('namespace_count,table_count'))

    const result = await resultQuery.first<{ n_count: number; t_count: number }>()

    if (!result) {
      return {
        namespaces: 0,
        tables: 0,
      }
    }

    return {
      namespaces: result.n_count,
      tables: result.t_count,
    }
  }

  findTableByLocation(params: { tenantId: string; location: string }): Promise<TableIndex> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .select<TableIndex[]>('id', 'name', 'namespace_id', 'location')

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', params.tenantId)
    }
    query.andWhere('location', params.location)
    return query.first<TableIndex>()
  }

  async dropTable(params: {
    name: string
    namespaceId: string
    catalogId: string
    tenantId: string
  }): Promise<void> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .where('name', params.name)
      .andWhere('namespace_id', params.namespaceId)
      .andWhere('catalog_id', params.catalogId)

    if (this.ops.multiTenant) {
      query.andWhere('tenant_id', params.tenantId)
    }

    return query.del()
  }

  async findCatalogByName(param: {
    tenantId: string
    name: string
    deleted?: boolean
  }): Promise<IcebergCatalog> {
    const table = this.ops.multiTenant ? 'iceberg_catalogs' : 'buckets_analytics'

    const query = this.db
      .withSchema(this.ops.schema)
      .table(table)
      .select<IcebergCatalog[]>('id', 'name')

    if (!param.deleted) {
      query.andWhere('deleted_at', null)
    }

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', param.tenantId)
    }

    query.andWhere('name', param.name)

    const result = await query.first<IcebergCatalog>()

    if (!result) {
      throw ERRORS.NoSuchCatalog(param.name)
    }
    return result
  }

  async countCatalogs(params: { tenantId: string; limit: number }): Promise<number> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_catalogs')
      .where('deleted_at', null)
      .limit(params.limit)
      .count<{ count: string }>('id as count')

    if (this.ops.multiTenant) {
      query.andWhere('tenant_id', params.tenantId)
    }

    const result = await query.first<{ count: string }>()
    return parseInt(result?.count || '0', 10)
  }

  async assignCatalog(params: {
    bucketName: string
    bucketId: string
    tenantId: string
  }): Promise<Catalog> {
    const catalog: Catalog = {
      id: params.bucketId,
      name: params.bucketName,
    }

    const conflictColumns = ['name']
    if (this.ops.multiTenant) {
      catalog['tenant_id'] = params.tenantId
      conflictColumns.push('tenant_id')
    }
    const result = await this.db
      .withSchema(this.ops.schema)
      .table('iceberg_catalogs')
      .insert(catalog)
      .onConflict(this.db.raw(`(${conflictColumns.join(', ')}) WHERE deleted_at IS NULL`))
      .merge({
        updated_at: new Date(),
      })
      .returning<Catalog[]>('*')

    if (result.length === 0) {
      throw ERRORS.NoSuchKey(params.bucketName)
    }

    return {
      ...catalog,
      id: result[0].id,
    }
  }

  async transaction<T>(
    callback: (trx: KnexMetastore) => Promise<T>,
    opts?: { isolationLevel?: Knex.IsolationLevels }
  ): Promise<T> {
    const tnx = await this.db.transaction(opts)
    tnx.on('query-error', (error: Error) => {
      throw DBError.fromError(error)
    })
    const storeInTransaction = new KnexMetastore(tnx, this.ops)

    try {
      const result = await callback(storeInTransaction)
      await tnx.commit()
      return result
    } catch (e) {
      await tnx.rollback()
      throw e
    }
  }

  async findNamespaceByName(params: {
    tenantId: string
    name: string
    catalogId: string
  }): Promise<NamespaceIndex> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_namespaces')
      .select<NamespaceIndex[]>('id', 'name', 'bucket_name', 'metadata')

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', params.tenantId)
    }

    query.andWhere('name', params.name)
    query.andWhere('catalog_id', params.catalogId)

    const result = await query.first<NamespaceIndex>()

    if (!result) {
      throw ERRORS.NoSuchKey(params.name)
    }
    return result
  }

  async dropNamespace(params: DropNamespaceParams): Promise<void> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_namespaces')
      .where('name', params.namespace)
      .andWhere('catalog_id', params.catalogId)

    if (this.ops.multiTenant) {
      query.andWhere('tenant_id', params.tenantId)
    }

    try {
      await query.del()
    } catch (error) {
      if (
        error instanceof Error &&
        (error.message.includes('RESTRICT') || error.message.includes('foreign key constraint'))
      ) {
        throw ERRORS.IcebergResourceNotEmpty('namespace', params.namespace)
      }
      throw DBError.fromError(error)
    }
  }

  async createNamespace(params: CreateNamespaceParams) {
    const namespaceIndex: Omit<NamespaceIndex, 'id'> = {
      name: params.name,
      catalog_id: params.bucketId,
      bucket_name: params.bucketName,
      metadata: params.metadata,
    }

    const conflictColumns = ['catalog_id', 'name']
    if (this.ops.multiTenant) {
      namespaceIndex['tenant_id'] = params.tenantId
      conflictColumns.unshift('tenant_id')
    }
    const result = await this.db
      .withSchema(this.ops.schema)
      .table('iceberg_namespaces')
      .insert(namespaceIndex)
      .onConflict(conflictColumns)
      .merge({
        updated_at: new Date(),
      })
      .returning<NamespaceIndex[]>('*')

    if (result.length === 0) {
      throw ERRORS.NoSuchKey(params.name)
    }

    return {
      ...namespaceIndex,
      id: result[0].id,
    }
  }

  async listNamespaces(params: ListNamespaceParams) {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_namespaces')
      .select<NamespaceIndex[]>('id', 'name', 'bucket_name')

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', params.tenantId)
    }

    query.andWhere('catalog_id', params.catalogId)

    return query
  }

  async createTable(params: CreateTableParams) {
    const tableIndex: Omit<TableIndex, 'id'> = {
      name: params.name,
      catalog_id: params.bucketId,
      bucket_name: params.bucketName,
      namespace_id: params.namespaceId,
      location: params.location,
      shard_key: params.shardKey,
      shard_id: params.shardId,
      remote_table_id: params.remoteTableId,
    }

    const conflictColumns = ['catalog_id', 'name', 'namespace_id']
    if (this.ops.multiTenant) {
      tableIndex['tenant_id'] = params.tenantId
      conflictColumns.unshift('tenant_id')
    }

    const result = await this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .insert(tableIndex)
      .onConflict(conflictColumns)
      .merge({
        updated_at: new Date(),
        location: params.location,
      })
      .returning<TableIndex[]>('*')

    if (result.length === 0) {
      throw ERRORS.NoSuchKey(params.name)
    }

    return result[0]
  }

  async findTableById(params: { tenantId: string; id: string; namespaceId: string }) {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .select<TableIndex[]>('id', 'name', 'namespace_id', 'location', 'shard_key', 'shard_id')
      .where('namespace_id', params.namespaceId)

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', params.tenantId)
    }

    query.andWhere('id', params.id)

    const result = await query.first<TableIndex>()

    if (!result) {
      throw ERRORS.NoSuchKey(params.id)
    }

    return result
  }

  async findTableByName(params: { tenantId: string; name: string; namespaceId: string }) {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .select<TableIndex[]>('id', 'name', 'namespace_id', 'location', 'shard_key', 'shard_id')

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', params.tenantId)
    }

    query.andWhere('name', params.name)
    query.andWhere('namespace_id', params.namespaceId)

    const result = await query.first<TableIndex>()

    if (!result) {
      throw ERRORS.NoSuchKey(params.name)
    }

    return result
  }

  async countTables(params: { namespaceId: string; tenantId?: string; limit: number }) {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .where('namespace_id', params.namespaceId)
      .limit(params.limit)
      .count<{ count: string }>('id as count')

    if (this.ops.multiTenant) {
      query.andWhere('tenant_id', params.tenantId)
    }

    const result = await query.first<{ count: string }>()
    return parseInt(result?.count || '0', 10)
  }

  async countNamespaces(param: { tenantId: string; limit: number }) {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_namespaces')
      .limit(param.limit)
      .count<{ count: string }>('id as count')

    if (this.ops.multiTenant) {
      query.andWhere('tenant_id', param.tenantId)
    }

    const result = await query.first<{ count: string }>()
    return parseInt(result?.count || '0', 10)
  }

  async findCatalogById(param: {
    id: string
    tenantId: string
    deleted?: boolean
  }): Promise<IcebergCatalog> {
    const table = this.ops.multiTenant ? 'iceberg_catalogs' : 'buckets_analytics'

    const query = this.db
      .withSchema(this.ops.schema)
      .table(table)
      .select<IcebergCatalog[]>('id', 'name')

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', param.tenantId)
    }

    query.andWhere('id', param.id)

    if (!param.deleted) {
      query.andWhere('deleted_at', null)
    }

    const catalog = await query.first<IcebergCatalog>()

    if (!catalog) {
      throw ERRORS.NoSuchCatalog(param.id)
    }
    return catalog
  }
}
