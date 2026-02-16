import { Knex } from 'knex'

export interface DuckLakeSchemaRow {
  schema_id: number
  schema_name: string
  path: string
  path_is_relative: boolean
  begin_snapshot: number
  end_snapshot: number | null
}

export interface DuckLakeTableRow {
  table_id: number
  schema_id: number
  table_name: string
  path: string
  path_is_relative: boolean
  begin_snapshot: number
  end_snapshot: number | null
}

export interface DuckLakeColumnRow {
  column_id: number
  table_id: number
  column_name: string
  column_type: string
  column_order: number
  default_value: string | null
  begin_snapshot: number
  end_snapshot: number | null
}

export interface DuckLakeSnapshotRow {
  snapshot_id: number
  snapshot_time: string
  schema_version: number | null
  next_catalog_id: number | null
  next_file_id: number | null
}

export interface DuckLakeDataFileRow {
  data_file_id: number
  table_id: number
  path: string
  path_is_relative: boolean
  file_format: string
  record_count: number
  file_size_bytes: number
  footer_size: number | null
  begin_snapshot: number
  end_snapshot: number | null
  file_order: number
  row_id_start: number | null
  partition_id: number | null
}

export interface DuckLakeDeleteFileRow {
  delete_file_id: number
  table_id: number
  data_file_id: number
  path: string
  path_is_relative: boolean
  format: string
  delete_count: number
  file_size_bytes: number
  footer_size: number | null
  begin_snapshot: number
  end_snapshot: number | null
}

export interface DuckLakeStatsRow {
  data_file_id: number
  table_id: number
  column_id: number
  column_size_bytes: number | null
  value_count: number | null
  null_count: number | null
  min_value: string | null
  max_value: string | null
  contains_nan: boolean | null
}

export class DuckLakeQueries {
  constructor(
    private readonly db: Knex,
    private readonly schema: string
  ) {}

  private table(name: string) {
    return this.db(`${this.schema}.${name}`)
  }

  async getDataPath(): Promise<string> {
    const row = await this.table('ducklake_metadata')
      .where('key', 'data_path')
      .first<{ key: string; value: string }>()
    return row?.value || ''
  }

  async findSchemaByName(name: string): Promise<DuckLakeSchemaRow | undefined> {
    return this.table('ducklake_schema')
      .where('schema_name', name)
      .whereNull('end_snapshot')
      .first<DuckLakeSchemaRow>()
  }

  async listSchemas(): Promise<DuckLakeSchemaRow[]> {
    return this.table('ducklake_schema').whereNull('end_snapshot').select<DuckLakeSchemaRow[]>('*')
  }

  async findTableByName(
    schemaId: number,
    tableName: string
  ): Promise<DuckLakeTableRow | undefined> {
    return this.table('ducklake_table')
      .where('schema_id', schemaId)
      .where('table_name', tableName)
      .whereNull('end_snapshot')
      .first<DuckLakeTableRow>()
  }

  async listTables(schemaId: number): Promise<DuckLakeTableRow[]> {
    return this.table('ducklake_table')
      .where('schema_id', schemaId)
      .whereNull('end_snapshot')
      .select<DuckLakeTableRow[]>('*')
  }

  async getColumns(tableId: number): Promise<DuckLakeColumnRow[]> {
    return this.table('ducklake_column')
      .where('table_id', tableId)
      .whereNull('end_snapshot')
      .orderBy('column_order', 'asc')
      .select<DuckLakeColumnRow[]>('*')
  }

  async getLatestSnapshot(): Promise<DuckLakeSnapshotRow | undefined> {
    return this.table('ducklake_snapshot')
      .orderBy('snapshot_id', 'desc')
      .first<DuckLakeSnapshotRow>()
  }

  async getSnapshots(): Promise<DuckLakeSnapshotRow[]> {
    return this.table('ducklake_snapshot')
      .orderBy('snapshot_id', 'asc')
      .select<DuckLakeSnapshotRow[]>('*')
  }

  async getDataFiles(tableId: number, snapshotId: number): Promise<DuckLakeDataFileRow[]> {
    return this.table('ducklake_data_file')
      .where('table_id', tableId)
      .where('begin_snapshot', '<=', snapshotId)
      .andWhere(function () {
        this.whereNull('end_snapshot').orWhere('end_snapshot', '>', snapshotId)
      })
      .select<DuckLakeDataFileRow[]>('*')
  }

  async getDeleteFiles(tableId: number, snapshotId: number): Promise<DuckLakeDeleteFileRow[]> {
    return this.table('ducklake_delete_file')
      .where('table_id', tableId)
      .where('begin_snapshot', '<=', snapshotId)
      .andWhere(function () {
        this.whereNull('end_snapshot').orWhere('end_snapshot', '>', snapshotId)
      })
      .select<DuckLakeDeleteFileRow[]>('*')
  }

  async getTablePathPrefix(tableId: number): Promise<string> {
    const table = await this.table('ducklake_table')
      .where('table_id', tableId)
      .whereNull('end_snapshot')
      .first<DuckLakeTableRow>()
    if (!table) return ''

    const schema = await this.table('ducklake_schema')
      .where('schema_id', table.schema_id)
      .whereNull('end_snapshot')
      .first<DuckLakeSchemaRow>()

    let prefix = ''
    if (schema?.path) prefix += schema.path
    if (table.path) prefix += table.path
    return prefix
  }

  async getFileColumnStatistics(
    dataFileIds: number[],
    tableId: number
  ): Promise<DuckLakeStatsRow[]> {
    if (dataFileIds.length === 0) return []
    return this.table('ducklake_file_column_stats')
      .whereIn('data_file_id', dataFileIds)
      .select<DuckLakeStatsRow[]>('*')
  }
}
