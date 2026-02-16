import avro from 'avsc'
import { Knex } from 'knex'
import { DuckLakeQueries, DuckLakeDataFileRow, DuckLakeStatsRow } from './ducklake-queries'
import { DuckLakeColumn, mapDuckLakeType } from './ducklake-types'
import { FieldType } from './rest-catalog-client'

// In-memory cache keyed by tableId:snapshotId
const manifestCache = new Map<string, { manifestList: Buffer; manifests: Map<string, Buffer> }>()

// Iceberg manifest list Avro schema (v2) - for binary encoding (avsc ignores field-id)
const MANIFEST_LIST_AVRO_DEF: any = {
  type: 'record',
  name: 'manifest_file',
  fields: [
    { name: 'manifest_path', type: 'string' },
    { name: 'manifest_length', type: 'long' },
    { name: 'partition_spec_id', type: 'int' },
    { name: 'content', type: 'int' },
    { name: 'sequence_number', type: 'long' },
    { name: 'min_sequence_number', type: 'long' },
    { name: 'added_snapshot_id', type: 'long' },
    { name: 'added_files_count', type: 'int' },
    { name: 'existing_files_count', type: 'int' },
    { name: 'deleted_files_count', type: 'int' },
    { name: 'added_rows_count', type: 'long' },
    { name: 'existing_rows_count', type: 'long' },
    { name: 'deleted_rows_count', type: 'long' },
    {
      name: 'partitions',
      type: ['null', { type: 'array', items: { type: 'record', name: 'r508', fields: [
        { name: 'contains_null', type: 'boolean' },
        { name: 'contains_nan', type: ['null', 'boolean'], default: null },
        { name: 'lower_bound', type: ['null', 'bytes'], default: null },
        { name: 'upper_bound', type: ['null', 'bytes'], default: null },
      ]}}],
      default: null,
    },
    { name: 'key_metadata', type: ['null', 'bytes'], default: null },
  ],
}

const MANIFEST_LIST_SCHEMA = avro.Type.forSchema(MANIFEST_LIST_AVRO_DEF)

// Raw JSON schema with field-id annotations for the OCF header
const MANIFEST_LIST_FIELD_IDS: Record<string, number> = {
  manifest_path: 500,
  manifest_length: 501,
  partition_spec_id: 502,
  content: 517,
  sequence_number: 515,
  min_sequence_number: 516,
  added_snapshot_id: 503,
  added_files_count: 504,
  existing_files_count: 505,
  deleted_files_count: 506,
  added_rows_count: 512,
  existing_rows_count: 513,
  deleted_rows_count: 514,
  partitions: 507,
  key_metadata: 519,
}

const PARTITION_SUMMARY_FIELD_IDS: Record<string, number> = {
  contains_null: 509,
  contains_nan: 518,
  lower_bound: 510,
  upper_bound: 511,
}

function buildManifestListSchemaJson(): string {
  const schema = JSON.parse(JSON.stringify(MANIFEST_LIST_AVRO_DEF))
  for (const field of schema.fields) {
    const fid = MANIFEST_LIST_FIELD_IDS[field.name]
    if (fid !== undefined) field['field-id'] = fid
    // Add field-ids to partition summary sub-record
    if (field.name === 'partitions' && Array.isArray(field.type)) {
      const arrayType = field.type.find((t: any) => typeof t === 'object' && t.type === 'array')
      if (arrayType?.items?.fields) {
        field['element-id'] = 508
        for (const subField of arrayType.items.fields) {
          const sfid = PARTITION_SUMMARY_FIELD_IDS[subField.name]
          if (sfid !== undefined) subField['field-id'] = sfid
        }
      }
    }
  }
  return JSON.stringify(schema)
}

const MANIFEST_LIST_SCHEMA_JSON = buildManifestListSchemaJson()

// Manifest entry field IDs
const MANIFEST_ENTRY_FIELD_IDS: Record<string, number> = {
  status: 0,
  snapshot_id: 1,
  sequence_number: 3,
  file_sequence_number: 4,
  data_file: 2,
}

const DATA_FILE_FIELD_IDS: Record<string, number> = {
  content: 134,
  file_path: 100,
  file_format: 101,
  partition: 102,
  record_count: 103,
  file_size_in_bytes: 104,
  column_sizes: 108,
  value_counts: 109,
  null_value_counts: 110,
  nan_value_counts: 137,
  lower_bounds: 125,
  upper_bounds: 128,
  key_metadata: 131,
  split_offsets: 132,
  equality_ids: 135,
  sort_order_id: 140,
}

// Iceberg int→long map as array of key-value records (Avro maps only support string keys)
function icebergIntLongMapArray(name: string) {
  return {
    type: 'array' as const,
    items: {
      type: 'record' as const,
      name,
      fields: [
        { name: 'key', type: 'int' as const },
        { name: 'value', type: 'long' as const },
      ],
    },
  }
}

// Iceberg int→bytes map as array of key-value records
function icebergIntBytesMapArray(name: string) {
  return {
    type: 'array' as const,
    items: {
      type: 'record' as const,
      name,
      fields: [
        { name: 'key', type: 'int' as const },
        { name: 'value', type: 'bytes' as const },
      ],
    },
  }
}

// Convert { "colId": value } object to [{ key: colId, value: value }, ...]
function toKeyValueArray(obj: Record<string, any>): { key: number; value: any }[] {
  return Object.entries(obj).map(([k, v]) => ({ key: parseInt(k, 10), value: v }))
}

function buildManifestEntrySchema(columns: DuckLakeColumn[]): { type: avro.Type; schemaJson: string } {
  const schemaDef: any = {
    type: 'record',
    name: 'manifest_entry',
    fields: [
      { name: 'status', type: 'int' },
      { name: 'snapshot_id', type: ['null', 'long'], default: null },
      { name: 'sequence_number', type: ['null', 'long'], default: null },
      { name: 'file_sequence_number', type: ['null', 'long'], default: null },
      {
        name: 'data_file',
        type: {
          type: 'record',
          name: 'r2',
          fields: [
            { name: 'content', type: 'int' },
            { name: 'file_path', type: 'string' },
            { name: 'file_format', type: 'string' },
            {
              name: 'partition',
              type: { type: 'record', name: 'r102', fields: [] },
            },
            { name: 'record_count', type: 'long' },
            { name: 'file_size_in_bytes', type: 'long' },
            { name: 'column_sizes', type: ['null', icebergIntLongMapArray('k108_v109')], default: null },
            { name: 'value_counts', type: ['null', icebergIntLongMapArray('k109_v110')], default: null },
            { name: 'null_value_counts', type: ['null', icebergIntLongMapArray('k110_v111')], default: null },
            { name: 'nan_value_counts', type: ['null', icebergIntLongMapArray('k138_v139')], default: null },
            { name: 'lower_bounds', type: ['null', icebergIntBytesMapArray('k125_v126')], default: null },
            { name: 'upper_bounds', type: ['null', icebergIntBytesMapArray('k128_v129')], default: null },
            { name: 'key_metadata', type: ['null', 'bytes'], default: null },
            { name: 'split_offsets', type: ['null', { type: 'array', items: 'long' }], default: null },
            { name: 'equality_ids', type: ['null', { type: 'array', items: 'int' }], default: null },
            { name: 'sort_order_id', type: ['null', 'int'], default: null },
          ],
        },
      },
    ],
  }

  const type = avro.Type.forSchema(schemaDef)

  // Build schema JSON with field-id annotations and logicalType: "map" for int-keyed maps
  const schemaWithIds = JSON.parse(JSON.stringify(schemaDef))
  for (const field of schemaWithIds.fields) {
    const fid = MANIFEST_ENTRY_FIELD_IDS[field.name]
    if (fid !== undefined) field['field-id'] = fid
    if (field.name === 'data_file' && typeof field.type === 'object' && field.type.fields) {
      for (const subField of field.type.fields) {
        const sfid = DATA_FILE_FIELD_IDS[subField.name]
        if (sfid !== undefined) subField['field-id'] = sfid
        // Add logicalType: "map" and key/value field-ids for Iceberg int-keyed maps
        if (['column_sizes', 'value_counts', 'null_value_counts', 'nan_value_counts', 'lower_bounds', 'upper_bounds'].includes(subField.name)) {
          const mapFieldIds: Record<string, [number, number]> = {
            column_sizes: [108, 109],
            value_counts: [109, 110],
            null_value_counts: [110, 111],
            nan_value_counts: [138, 139],
            lower_bounds: [125, 126],
            upper_bounds: [128, 129],
          }
          const [keyId, valueId] = mapFieldIds[subField.name] || [0, 0]
          // Find the array type in the union
          const unionTypes = Array.isArray(subField.type) ? subField.type : [subField.type]
          for (const ut of unionTypes) {
            if (typeof ut === 'object' && ut.type === 'array') {
              ut['logicalType'] = 'map'
              if (ut.items?.fields) {
                for (const kvField of ut.items.fields) {
                  if (kvField.name === 'key') kvField['field-id'] = keyId
                  if (kvField.name === 'value') kvField['field-id'] = valueId
                }
              }
            }
          }
        }
      }
    }
  }
  const schemaJson = JSON.stringify(schemaWithIds)

  return { type, schemaJson }
}

function encodeStatValue(value: string | null, icebergType: FieldType): Buffer | null {
  if (value === null || value === undefined) return null

  if (typeof icebergType !== 'string') return null

  switch (icebergType) {
    case 'long': {
      const buf = Buffer.alloc(8)
      const n = BigInt(value)
      buf.writeBigInt64LE(n)
      return buf
    }
    case 'integer': {
      const buf = Buffer.alloc(4)
      buf.writeInt32LE(parseInt(value, 10))
      return buf
    }
    case 'float': {
      const buf = Buffer.alloc(4)
      buf.writeFloatLE(parseFloat(value))
      return buf
    }
    case 'double': {
      const buf = Buffer.alloc(8)
      buf.writeDoubleLE(parseFloat(value))
      return buf
    }
    case 'string': {
      return Buffer.from(value, 'utf-8')
    }
    case 'boolean': {
      return Buffer.from([value === 'true' ? 1 : 0])
    }
    case 'date': {
      // Days since epoch
      const buf = Buffer.alloc(4)
      const days = Math.floor(new Date(value).getTime() / (86400 * 1000))
      buf.writeInt32LE(days)
      return buf
    }
    case 'timestamp':
    case 'timestamptz': {
      // Microseconds since epoch
      const buf = Buffer.alloc(8)
      const ms = new Date(value).getTime()
      buf.writeBigInt64LE(BigInt(ms) * 1000n)
      return buf
    }
    default:
      return Buffer.from(value, 'utf-8')
  }
}

function writeAvroOCF(schema: avro.Type, records: any[], metadata: Record<string, string>, rawSchemaJson?: string): Buffer {
  const avroSchemaJson = rawSchemaJson || JSON.stringify(schema.schema())

  // Build metadata block
  const metaEntries: Record<string, Buffer> = {
    'avro.schema': Buffer.from(avroSchemaJson),
  }
  for (const [k, v] of Object.entries(metadata)) {
    metaEntries[k] = Buffer.from(v)
  }

  // Use avsc to encode to a buffer through block encoder
  const chunks: Buffer[] = []

  // Magic
  chunks.push(Buffer.from([0x4f, 0x62, 0x6a, 0x01]))

  // File header metadata (map)
  const metaKeys = Object.keys(metaEntries)
  chunks.push(encodeLong(metaKeys.length))
  for (const key of metaKeys) {
    const keyBuf = Buffer.from(key)
    chunks.push(encodeLong(keyBuf.length))
    chunks.push(keyBuf)
    const valBuf = metaEntries[key]
    chunks.push(encodeLong(valBuf.length))
    chunks.push(valBuf)
  }
  chunks.push(encodeLong(0)) // end of map

  // Sync marker (16 random bytes)
  const syncMarker = Buffer.alloc(16)
  for (let i = 0; i < 16; i++) syncMarker[i] = Math.floor(Math.random() * 256)
  chunks.push(syncMarker)

  // Data block
  if (records.length > 0) {
    const encodedRecords: Buffer[] = []
    for (const record of records) {
      encodedRecords.push(schema.toBuffer(record))
    }
    const blockData = Buffer.concat(encodedRecords)

    chunks.push(encodeLong(records.length))
    chunks.push(encodeLong(blockData.length))
    chunks.push(blockData)
    chunks.push(syncMarker)
  }

  return Buffer.concat(chunks)
}

function encodeLong(n: number): Buffer {
  // Avro variable-length zig-zag encoding
  let val = (n << 1) ^ (n >> 31)
  const bytes: number[] = []
  while ((val & ~0x7f) !== 0) {
    bytes.push((val & 0x7f) | 0x80)
    val >>>= 7
  }
  bytes.push(val)
  return Buffer.from(bytes)
}

export interface DuckLakeAvroGeneratorOptions {
  db: Knex
  ducklakeSchema: string
  virtualPrefix: string
  dataBucket: string
}

export class DuckLakeAvroGenerator {
  private queries: DuckLakeQueries
  private virtualPrefix: string
  private dataBucket: string

  constructor(options: DuckLakeAvroGeneratorOptions) {
    this.queries = new DuckLakeQueries(options.db, options.ducklakeSchema)
    this.virtualPrefix = options.virtualPrefix
    this.dataBucket = options.dataBucket
  }

  async generate(virtualPath: string): Promise<Buffer> {
    // Parse path: __ducklake__/t<tableId>/s<snapshotId>/snap-<snapshotId>.avro (manifest list)
    // or: __ducklake__/t<tableId>/s<snapshotId>/m<N>.avro (manifest file)
    const prefixIdx = virtualPath.indexOf(this.virtualPrefix + '/')
    if (prefixIdx < 0) {
      throw new Error(`Invalid virtual path: ${virtualPath}`)
    }

    const relativePath = virtualPath.slice(prefixIdx + this.virtualPrefix.length + 1)
    const parts = relativePath.split('/')

    // Expected: t<tableId>/s<snapshotId>/<filename>
    const tableIdMatch = parts[0]?.match(/^t(\d+)$/)
    const snapshotIdMatch = parts[1]?.match(/^s(\d+)$/)
    const filename = parts[2]

    if (!tableIdMatch || !snapshotIdMatch || !filename) {
      throw new Error(`Invalid virtual path format: ${virtualPath}`)
    }

    const tableId = parseInt(tableIdMatch[1], 10)
    const snapshotId = parseInt(snapshotIdMatch[1], 10)

    const cacheKey = `${tableId}:${snapshotId}`

    if (!manifestCache.has(cacheKey)) {
      await this.buildAndCache(tableId, snapshotId, cacheKey)
    }

    const cached = manifestCache.get(cacheKey)!

    if (filename.startsWith('snap-')) {
      return cached.manifestList
    }

    const manifest = cached.manifests.get(filename)
    if (!manifest) {
      throw new Error(`Manifest file not found: ${filename}`)
    }
    return manifest
  }

  private async buildAndCache(tableId: number, snapshotId: number, cacheKey: string) {
    const dataPath = await this.queries.getDataPath()
    const tablePathPrefix = await this.queries.getTablePathPrefix(tableId)
    const columns = await this.queries.getColumns(tableId)
    const dataFiles = await this.queries.getDataFiles(tableId, snapshotId)
    const deleteFiles = await this.queries.getDeleteFiles(tableId, snapshotId)

    const dataFileIds = dataFiles.map((f) => f.data_file_id)
    const stats = await this.queries.getFileColumnStatistics(dataFileIds, tableId)
    const statsByFile = new Map<number, DuckLakeStatsRow[]>()
    for (const s of stats) {
      if (!statsByFile.has(s.data_file_id)) statsByFile.set(s.data_file_id, [])
      statsByFile.get(s.data_file_id)!.push(s)
    }

    // Build column type map
    const columnTypeMap = new Map<number, FieldType>()
    for (const col of columns) {
      columnTypeMap.set(col.column_id, mapDuckLakeType(col.column_type))
    }

    // Build Iceberg schema JSON for manifest metadata
    const icebergSchemaJson = JSON.stringify({
      type: 'struct',
      'schema-id': 0,
      fields: columns.map((c) => ({
        id: c.column_id,
        name: c.column_name,
        required: false,
        type: mapDuckLakeType(c.column_type),
      })),
    })

    const basePath = `s3://${this.dataBucket}/${this.virtualPrefix}/t${tableId}/s${snapshotId}`

    // Generate data manifest
    const dataManifestFilename = `${snapshotId}-m0.avro`
    const dataManifestPath = `${basePath}/${dataManifestFilename}`
    const dataManifestBuf = this.generateManifest(
      dataFiles,
      statsByFile,
      columnTypeMap,
      columns,
      dataPath,
      tablePathPrefix,
      snapshotId,
      0, // content=data
      icebergSchemaJson
    )

    const manifests = new Map<string, Buffer>()
    manifests.set(dataManifestFilename, dataManifestBuf)

    // Build manifest list entries
    const manifestListEntries: any[] = []

    const totalDataRecords = dataFiles.reduce((sum, f) => sum + f.record_count, 0)

    manifestListEntries.push({
      manifest_path: dataManifestPath,
      manifest_length: dataManifestBuf.length,
      partition_spec_id: 0,
      content: 0, // data
      sequence_number: snapshotId + 1,
      min_sequence_number: snapshotId + 1,
      added_snapshot_id: snapshotId,
      added_files_count: dataFiles.length,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: totalDataRecords,
      existing_rows_count: 0,
      deleted_rows_count: 0,
      partitions: null,
      key_metadata: null,
    })

    // Delete manifest if there are delete files
    if (deleteFiles.length > 0) {
      const deleteManifestFilename = `${snapshotId}-m1.avro`
      const deleteManifestPath = `${basePath}/${deleteManifestFilename}`
      const deleteManifestBuf = this.generateDeleteManifest(
        deleteFiles,
        dataPath,
        tablePathPrefix,
        snapshotId,
        icebergSchemaJson
      )
      manifests.set(deleteManifestFilename, deleteManifestBuf)

      const totalDeleteRecords = deleteFiles.reduce((sum, f) => sum + f.delete_count, 0)

      manifestListEntries.push({
        manifest_path: deleteManifestPath,
        manifest_length: deleteManifestBuf.length,
        partition_spec_id: 0,
        content: 1, // deletes
        sequence_number: snapshotId + 1,
        min_sequence_number: snapshotId + 1,
        added_snapshot_id: snapshotId,
        added_files_count: deleteFiles.length,
        existing_files_count: 0,
        deleted_files_count: 0,
        added_rows_count: totalDeleteRecords,
        existing_rows_count: 0,
        deleted_rows_count: 0,
        partitions: null,
        key_metadata: null,
      })
    }

    // Generate manifest list
    const manifestListBuf = writeAvroOCF(MANIFEST_LIST_SCHEMA, manifestListEntries, {
      'format-version': '2',
    }, MANIFEST_LIST_SCHEMA_JSON)

    manifestCache.set(cacheKey, {
      manifestList: manifestListBuf,
      manifests,
    })
  }

  private generateManifest(
    dataFiles: DuckLakeDataFileRow[],
    statsByFile: Map<number, DuckLakeStatsRow[]>,
    columnTypeMap: Map<number, FieldType>,
    columns: DuckLakeColumn[],
    dataPath: string,
    tablePathPrefix: string,
    snapshotId: number,
    content: number,
    icebergSchemaJson: string
  ): Buffer {
    const { type: manifestEntrySchema, schemaJson: manifestEntrySchemaJson } = buildManifestEntrySchema(columns)

    const entries = dataFiles.map((file) => {
      const filePath = file.path_is_relative
        ? `${dataPath}${tablePathPrefix}${file.path}`
        : file.path

      const fileStats = statsByFile.get(file.data_file_id) || []

      // Build column_sizes map
      const columnSizes: Record<string, number> = {}
      for (const col of columns) {
        columnSizes[String(col.column_id)] = Math.floor(file.file_size_bytes / columns.length)
      }

      // Build value_counts and null_value_counts
      const valueCounts: Record<string, number> = {}
      const nullValueCounts: Record<string, number> = {}
      for (const col of columns) {
        valueCounts[String(col.column_id)] = file.record_count
        nullValueCounts[String(col.column_id)] = 0
      }

      // Build lower/upper bounds from stats
      const lowerBounds: Record<string, Buffer> = {}
      const upperBounds: Record<string, Buffer> = {}
      for (const stat of fileStats) {
        const colType = columnTypeMap.get(stat.column_id)
        if (!colType) continue

        const lower = encodeStatValue(stat.min_value, colType)
        const upper = encodeStatValue(stat.max_value, colType)
        if (lower) lowerBounds[String(stat.column_id)] = lower
        if (upper) upperBounds[String(stat.column_id)] = upper

        if (stat.null_count !== null) {
          nullValueCounts[String(stat.column_id)] = stat.null_count
        }
        if (stat.value_count !== null) {
          valueCounts[String(stat.column_id)] = stat.value_count
        }
      }

      return {
        status: 1, // ADDED
        snapshot_id: snapshotId,
        sequence_number: snapshotId + 1,
        file_sequence_number: snapshotId + 1,
        data_file: {
          content,
          file_path: filePath,
          file_format: 'PARQUET',
          partition: {},
          record_count: file.record_count,
          file_size_in_bytes: file.file_size_bytes,
          column_sizes: toKeyValueArray(columnSizes),
          value_counts: toKeyValueArray(valueCounts),
          null_value_counts: toKeyValueArray(nullValueCounts),
          nan_value_counts: null,
          lower_bounds: Object.keys(lowerBounds).length > 0 ? toKeyValueArray(lowerBounds) : null,
          upper_bounds: Object.keys(upperBounds).length > 0 ? toKeyValueArray(upperBounds) : null,
          key_metadata: null,
          split_offsets: [4],
          equality_ids: null,
          sort_order_id: 0,
        },
      }
    })

    return writeAvroOCF(manifestEntrySchema, entries, {
      schema: icebergSchemaJson,
      'schema-id': '0',
      'partition-spec': '[]',
      'partition-spec-id': '0',
      'format-version': '2',
      content: content === 0 ? 'data' : 'deletes',
    }, manifestEntrySchemaJson)
  }

  private generateDeleteManifest(
    deleteFiles: any[],
    dataPath: string,
    tablePathPrefix: string,
    snapshotId: number,
    icebergSchemaJson: string
  ): Buffer {
    const { type: manifestEntrySchema, schemaJson: manifestEntrySchemaJson } = buildManifestEntrySchema([])

    const entries = deleteFiles.map((file: any) => {
      const filePath = file.path_is_relative
        ? `${dataPath}${tablePathPrefix}${file.path}`
        : file.path

      return {
        status: 1,
        snapshot_id: snapshotId,
        sequence_number: snapshotId + 1,
        file_sequence_number: snapshotId + 1,
        data_file: {
          content: 1,
          file_path: filePath,
          file_format: 'PARQUET',
          partition: {},
          record_count: file.delete_count,
          file_size_in_bytes: file.file_size_bytes,
          column_sizes: null,
          value_counts: null,
          null_value_counts: null,
          nan_value_counts: null,
          lower_bounds: null,
          upper_bounds: null,
          key_metadata: null,
          split_offsets: null,
          equality_ids: null,
          sort_order_id: null,
        },
      }
    })

    return writeAvroOCF(manifestEntrySchema, entries, {
      schema: icebergSchemaJson,
      'schema-id': '0',
      'partition-spec': '[]',
      'partition-spec-id': '0',
      'format-version': '2',
      content: 'deletes',
    }, manifestEntrySchemaJson)
  }
}

export function isDuckLakeVirtualPath(key: string, virtualPrefix: string): boolean {
  return key.includes(`${virtualPrefix}/`)
}
