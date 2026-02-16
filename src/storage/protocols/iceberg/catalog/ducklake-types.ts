import { Field, FieldType, PrimitiveType, Schema } from './rest-catalog-client'

export interface DuckLakeColumn {
  column_id: number
  table_id: number
  column_name: string
  column_type: string
  column_order: number
  default_value?: string | null
  begin_snapshot: number
  end_snapshot?: number | null
}

const DUCKLAKE_TO_ICEBERG_TYPE: Record<string, PrimitiveType> = {
  bigint: 'long',
  int64: 'long',
  long: 'long',
  integer: 'integer',
  int32: 'integer',
  int: 'integer',
  smallint: 'integer',
  tinyint: 'integer',
  float: 'float',
  float32: 'float',
  double: 'double',
  float64: 'double',
  boolean: 'boolean',
  bool: 'boolean',
  varchar: 'string',
  text: 'string',
  string: 'string',
  date: 'date',
  time: 'time',
  timestamp: 'timestamp',
  'timestamp with time zone': 'timestamptz',
  timestamptz: 'timestamptz',
  uuid: 'uuid',
  blob: 'binary',
  binary: 'binary',
}

export function mapDuckLakeType(duckType: string): FieldType {
  const normalized = duckType.toLowerCase().trim()
  const mapped = DUCKLAKE_TO_ICEBERG_TYPE[normalized]
  if (mapped) return mapped
  // Handle parameterized types like varchar(255), decimal(10,2)
  const base = normalized.split('(')[0].trim()
  const baseMatch = DUCKLAKE_TO_ICEBERG_TYPE[base]
  if (baseMatch) return baseMatch
  // Default to string for unmapped types
  return 'string'
}

export function buildIcebergField(col: DuckLakeColumn): Field {
  return {
    id: col.column_id,
    name: col.column_name,
    type: mapDuckLakeType(col.column_type),
    required: false,
  }
}

export function buildIcebergSchema(columns: DuckLakeColumn[], schemaId?: number): Schema {
  return {
    type: 'struct',
    fields: columns.map(buildIcebergField),
    'schema-id': schemaId ?? 0,
    'identifier-field-ids': [],
  }
}
