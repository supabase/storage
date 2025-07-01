import { Knex } from 'knex'
import { ERRORS } from '@internal/errors'

export interface AssignInterfaceParams {
  name: string
  bucketId: string
  tenantId: string
}

export interface ListNamespaceParams {
  tenantId: string
  bucketId: string
}

export interface NamespaceIndex {
  id: string
  name: string
  bucket_id: string
  tenant_id?: string
}

export interface TableIndex {
  id: string
  name: string
  namespace_id: string
  location: string
  tenant_id?: string
}

export interface DropNamespaceParams {
  name: string
  bucketId: string
  tenantId: string
}

export interface CreateTableParams {
  name: string
  namespaceId: string
  tenantId: string
  location: string
}

export interface Metastore {
  assignNamespace(params: AssignInterfaceParams): Promise<NamespaceIndex>
  listNamespaces(params: ListNamespaceParams): Promise<NamespaceIndex[]>
  dropNamespace(params: DropNamespaceParams): Promise<void>
  createTable(params: CreateTableParams): Promise<TableIndex>
  findTableById(params: { tenantId: string; id: string }): Promise<TableIndex>
  findTableByName(params: { tenantId: string; name: string }): Promise<TableIndex>
  findNamespaceByName(params: { tenantId: string; name: string }): Promise<NamespaceIndex>
}

export class KnexMetastore implements Metastore {
  constructor(
    private readonly db: Knex,
    private readonly ops: { schema: string; storeTenantId?: boolean }
  ) {}

  async findNamespaceByName(params: { tenantId: string; name: string }): Promise<NamespaceIndex> {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_namespaces')
      .select<NamespaceIndex[]>('id', 'name', 'bucket_id')

    if (this.ops.storeTenantId) {
      query.select('tenant_id')
      query.andWhere('tenant_id', params.tenantId)
    }

    query.andWhere('name', params.name)

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
      .where('name', params.name)
      .andWhere('bucket_id', params.bucketId)

    if (this.ops.storeTenantId) {
      query.andWhere('tenant_id', params.tenantId)
    }

    return query.del()
  }

  async assignNamespace(params: AssignInterfaceParams) {
    const namespaceIndex: Omit<NamespaceIndex, 'id'> = {
      name: params.name,
      bucket_id: params.bucketId,
    }

    const conflictColumns = ['name', 'bucket_id']
    if (this.ops.storeTenantId) {
      namespaceIndex['tenant_id'] = params.tenantId
      conflictColumns.push('tenant_id')
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

    if (this.ops.storeTenantId) {
      query.select('tenant_id')
      query.andWhere('tenant_id', params.tenantId)
    }

    query.andWhere('bucket_id', params.bucketId)

    return query
  }

  async createTable(params: CreateTableParams) {
    const tableIndex: Omit<TableIndex, 'id'> = {
      name: params.name,
      namespace_id: params.namespaceId,
      location: params.location,
    }

    const conflictColumns = ['name', 'namespace_id']
    if (this.ops.storeTenantId) {
      tableIndex['tenant_id'] = params.tenantId
      conflictColumns.push('tenant_id')
    }

    return this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .insert(tableIndex)
      .onConflict(conflictColumns)
      .merge({
        updated_at: new Date(),
        location: params.location,
      })
      .returning<TableIndex>('*')
  }

  async findTableById(params: { tenantId: string; id: string }) {
    const query = this.db
      .withSchema(this.ops.schema)
      .table('iceberg_tables')
      .select<TableIndex[]>('id', 'name', 'namespace_id', 'location')

    if (this.ops.storeTenantId) {
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

    if (this.ops.storeTenantId) {
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
}
