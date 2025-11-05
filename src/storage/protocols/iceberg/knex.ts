import { Knex } from 'knex'
import { ERRORS } from '@internal/errors'
import { IcebergCatalog } from '@storage/schemas'
import { DBError } from '@storage/database'
import { DropTableRequest } from '@storage/protocols/iceberg/catalog'

export interface AssignInterfaceParams {
  name: string
  bucketId: string
  tenantId?: string
}

export interface ListNamespaceParams {
  tenantId?: string
  bucketId: string
}

export interface NamespaceIndex {
  id: string
  name: string
  bucket_id: string
  tenant_id?: string
}

export interface Catalog {
  id: string
  tenant_id?: string
}

export interface TableIndex {
  id: string
  name: string
  bucket_id: string
  namespace_id: string
  location: string
  tenant_id?: string
}

export interface DropNamespaceParams {
  namespace: string
  bucketId: string
  tenantId?: string
}

export interface CreateTableParams {
  name: string
  bucketId: string
  namespaceId: string
  tenantId?: string
  location: string
}

export interface Metastore {
  assignNamespace(params: AssignInterfaceParams): Promise<NamespaceIndex>
  listNamespaces(params: ListNamespaceParams): Promise<NamespaceIndex[]>
  dropNamespace(params: DropNamespaceParams): Promise<void>
  dropCatalog(params: { tenantId?: string; bucketId: string }): Promise<boolean>
  createTable(params: CreateTableParams): Promise<TableIndex>
  dropTable(params: DropTableRequest): Promise<void>
  findTableByLocation(params: { tenantId?: string; location: string }): Promise<TableIndex>
  findTableById(params: { tenantId?: string; namespaceId: string; id: string }): Promise<TableIndex>
  findTableByName(params: { tenantId?: string; name: string }): Promise<TableIndex>
  findNamespaceByName(params: {
    tenantId: string
    bucketId: string
    name: string
  }): Promise<NamespaceIndex>
  transaction<T>(
    callback: (trx: KnexMetastore) => Promise<T>,
    opts?: { isolationLevel?: Knex.IsolationLevels }
  ): Promise<T>

  assignCatalog(param: { bucketId: string; tenantId: string }): Promise<any>
  countCatalogs(params: { tenantId: string; limit: number }): Promise<number>
  countNamespaces(param: { tenantId: string; limit: number }): Promise<number>
  countTables(params: { namespaceId: string; tenantId?: string; limit: number }): Promise<number>
  countResources(params: {
    tenantId?: string
    limit: number
  }): Promise<{ namespaces: number; tables: number }>
  findCatalogById(param: { tenantId: string; id: string }): Promise<IcebergCatalog>

  listTables(param: {
    tenantId: string
    pageSize: number | undefined
    namespaceId: string
  }): Promise<TableIndex[]>
}

export class KnexMetastore implements Metastore {
  constructor(
    private readonly db: Knex | Knex.Transaction,
    private readonly ops: { schema: string; multiTenant?: boolean }
  ) {}

  async dropCatalog(params: { tenantId?: string | undefined; bucketId: string }): Promise<boolean> {
    if (!this.ops.multiTenant) {
      return Promise.resolve(false)
    }

    await this.db
      .withSchema(this.ops.schema)
      .table('iceberg_catalogs')
      .where('tenant_id', params.tenantId)
      .andWhere('id', params.bucketId)
      .del()

    return true
  }

  listTables(param: {
    tenantId: string
    pageSize: number | undefined
    namespaceId: string
  }): Promise<TableIndex[]> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .select<TableIndex[]>('id', 'name', 'namespace_id')
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
      .where('bucket_id', params.bucketId)
      .limit(params.limit)
      .count<{ count: string }>('id as n_count')

    if (this.ops.multiTenant) {
      countNamespaces.andWhere('tenant_id', params.tenantId)
    }

    const countTables = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .where('bucket_id', params.bucketId)
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

  async dropTable(params: DropTableRequest): Promise<void> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .where('name', params.table)
      .andWhere('namespace_id', params.namespace)
      .andWhere('bucket_id', params.warehouse)

    if (this.ops.multiTenant) {
      query.andWhere('tenant_id', params.tenantId)
    }

    return query.del()
  }

  async findCatalogById(param: { tenantId: string; id: string }): Promise<IcebergCatalog> {
    const table = this.ops.multiTenant ? 'iceberg_catalogs' : 'buckets_analytics'

    const query = this.db.withSchema(this.ops.schema).table(table).select<IcebergCatalog[]>('id')

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', param.tenantId)
    }

    query.andWhere('id', param.id)

    const result = await query.first<IcebergCatalog>()

    if (!result) {
      throw ERRORS.NoSuchCatalog(param.id)
    }
    return result
  }

  async countCatalogs(params: { tenantId: string; limit: number }): Promise<number> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_catalogs')
      .limit(params.limit)
      .count<{ count: string }>('id as count')

    if (this.ops.multiTenant) {
      query.andWhere('tenant_id', params.tenantId)
    }

    const result = await query.first<{ count: string }>()
    return parseInt(result?.count || '0', 10)
  }

  async assignCatalog(params: { bucketId: string; tenantId: string }): Promise<Catalog> {
    const catalog: Catalog = {
      id: params.bucketId,
    }

    const conflictColumns = ['id']
    if (this.ops.multiTenant) {
      catalog['tenant_id'] = params.tenantId
      conflictColumns.push('tenant_id')
    }
    const result = await this.db
      .withSchema(this.ops.schema)
      .table('iceberg_catalogs')
      .insert(catalog)
      .onConflict(conflictColumns)
      .merge({
        updated_at: new Date(),
      })
      .returning<Catalog[]>('*')

    if (result.length === 0) {
      throw ERRORS.NoSuchKey(params.bucketId)
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
    bucketId: string
  }): Promise<NamespaceIndex> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_namespaces')
      .select<NamespaceIndex[]>('id', 'name', 'bucket_id')

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', params.tenantId)
    }

    query.andWhere('name', params.name)
    query.andWhere('bucket_id', params.bucketId)

    const result = await query.first<NamespaceIndex>()

    if (!result) {
      throw ERRORS.NoSuchKey(params.name)
    }
    return result
  }

  dropNamespace(params: DropNamespaceParams): Promise<void> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_namespaces')
      .where('name', params.namespace)
      .andWhere('bucket_id', params.bucketId)

    if (this.ops.multiTenant) {
      query.andWhere('tenant_id', params.tenantId)
    }

    return query.del()
  }

  async assignNamespace(params: AssignInterfaceParams) {
    const namespaceIndex: Omit<NamespaceIndex, 'id'> = {
      name: params.name,
      bucket_id: params.bucketId,
    }

    const conflictColumns = ['bucket_id', 'name']
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
      .select<NamespaceIndex[]>('name', 'bucket_id')

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', params.tenantId)
    }

    query.andWhere('bucket_id', params.bucketId)

    return query
  }

  async createTable(params: CreateTableParams) {
    const tableIndex: Omit<TableIndex, 'id'> = {
      name: params.name,
      bucket_id: params.bucketId,
      namespace_id: params.namespaceId,
      location: params.location,
    }

    const conflictColumns = ['name', 'namespace_id']
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
      .select<TableIndex[]>('id', 'name', 'namespace_id', 'location')
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

  async findTableByName(params: { tenantId: string; name: string }) {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .select<TableIndex[]>('id', 'name', 'namespace_id', 'location')

    if (this.ops.multiTenant) {
      query.select('tenant_id')
      query.andWhere('tenant_id', params.tenantId)
    }

    query.andWhere('name', params.name)

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
}
