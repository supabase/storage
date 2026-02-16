# DuckLake-to-Iceberg REST Catalog Bridge

## Context

DuckLake stores Iceberg-equivalent metadata (schemas, snapshots, data files, statistics) in PostgreSQL tables instead of metadata.json + Avro manifest files in object storage. The goal is to modify the Supabase Storage API so that Iceberg REST catalog requests are fulfilled from DuckLake's PostgreSQL metadata instead of proxying to an upstream Iceberg catalog. Data files (parquet) remain real in S3 — only the metadata layer is virtual.

**Scope**: Read-only path for `SELECT * FROM table` via DuckDB's Iceberg extension. Single tenant mode only.

## Request Flow (8 requests from DuckDB for a SELECT query)

1. `GET /iceberg/v1/config?warehouse=warehouse` → catalog config JSON
2. `GET /iceberg/v1/warehouse/namespaces/default` → namespace info JSON
3. `GET /iceberg/v1/warehouse/namespaces/default/tables/taxi_dataset` (×4) → full table metadata JSON (schema, snapshots, manifest-list paths)
4. `GET /s3/<bucket>/.../snap-XXX.avro` → manifest list (Avro binary) — **VIRTUAL**
5. `GET /s3/<bucket>/.../XXX-m0.avro` → manifest file (Avro binary) — **VIRTUAL**
6. `GET /s3/<bucket>/.../XXX.parquet` (×2, Range) → data files — **REAL, no change needed**

## Implementation Plan

### 1. Config (`src/config.ts`)

Add new env vars:
```
ICEBERG_CATALOG_MODE: 'rest' | 'ducklake'  (default: 'rest')
DUCKLAKE_SCHEMA: string                     (default: 'public')
DUCKLAKE_VIRTUAL_PREFIX: string             (default: '__ducklake__')
DUCKLAKE_DATA_BUCKET: string               (default: 'data-lake') — the customer's Supabase storage bucket for DuckLake data files
```

### 2. DuckLake Queries Module (NEW: `src/storage/protocols/iceberg/catalog/ducklake-queries.ts`)

SQL queries against `ducklake_*` tables via Knex. Key methods:
- `getDataPath()` → reads `ducklake_metadata` where key='data_path' → `"s3://<DUCKLAKE_DATA_BUCKET>/"` (bucket name from config)
- `findSchemaByName(name)` → `ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`
- `listSchemas()` → all active schemas
- `findTableByName(schemaId, tableName)` → `ducklake_table WHERE ...`
- `listTables(schemaId)` → active tables in schema
- `getColumns(tableId)` → `ducklake_column WHERE table_id = ? AND end_snapshot IS NULL ORDER BY column_order`
- `getLatestSnapshot()` → `ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1`
- `getSnapshots()` → all snapshots
- `getDataFiles(tableId, snapshotId)` → files visible at snapshot: `begin_snapshot <= S AND (end_snapshot IS NULL OR end_snapshot > S)`
- `getDeleteFiles(tableId, snapshotId)` → same visibility logic
- `getFileColumnStatistics(dataFileIds, tableId)` → join with `ducklake_file_column_statistics`

### 3. Type Mapping (NEW: `src/storage/protocols/iceberg/catalog/ducklake-types.ts`)

DuckLake type → Iceberg type mapping:
| DuckLake | Iceberg |
|----------|---------|
| int64 | long |
| float64 | double |
| timestamp | timestamp |
| varchar | string |
| int32 | integer |
| boolean | boolean |
| date | date |
| uuid | uuid |

Also: `buildIcebergSchema(columns)` → builds the `Schema` object with fields, and `buildIcebergField(col)` → maps a single column to an Iceberg `Field`.

### 4. DuckLake Catalog (NEW: `src/storage/protocols/iceberg/catalog/ducklake-catalog.ts`)

Implements the same methods the route handlers call on `req.icebergCatalog`:

**`getConfig(params)`** — Returns config with the warehouse prefix set to the customer's analytics bucket name. In DuckLake mode there is no multi-catalog concept — each customer gets a single catalog, and the "warehouse" name is the analytics bucket name that was created for the customer. The prefix is used by DuckDB to scope subsequent REST requests:
```json
{ "defaults": { "prefix": "<analytics-bucket-name>", ... }, "overrides": { "prefix": "<analytics-bucket-name>" } }
```

**`loadNamespaceMetadata(params)`** — Queries `ducklake_schema` by name → returns `{ namespace: [name], properties: {} }`

**`listNamespaces(params)`** — Queries active schemas → returns `{ namespaces: [[name1], [name2]] }`

**`listTables(params)`** — Queries active tables in schema → returns identifiers list

**`loadTable(params)`** — The core method. Builds full Iceberg table metadata JSON:
1. Find schema (namespace) by name
2. Find table by name within schema
3. Get columns → build Iceberg schema with type mapping
4. Get latest snapshot + all snapshots
5. Get data files + delete files for summary stats
6. Build virtual manifest-list path: `s3://<bucket>/__ducklake__/t<tableId>/s<snapshotId>/snap-<snapshotId>.avro`
7. Build table location: resolved from `ducklake_metadata.data_path`
8. Return `LoadTableResult` with complete metadata including: `format-version: 2`, `table-uuid`, `schemas`, `current-snapshot-id`, `snapshots` (with virtual manifest-list path), `partition-specs: [{ spec-id: 0, fields: [] }]`, `sort-orders: [{ order-id: 0, fields: [] }]`, `refs: { main: { snapshot-id, type: "branch" } }`

**`findTableByLocation(params)`** — Used by `detectS3IcebergBucket` plugin. Query `ducklake_table` joined with `ducklake_schema` to find table by S3 location prefix. Return a compatible `TableIndex` object.

**`namespaceExists`**, **`tableExists`** — Simple existence checks against DuckLake tables.

### 5. Avro Generation (NEW: `src/storage/protocols/iceberg/catalog/ducklake-avro.ts`)

Uses `avsc` npm package to generate Iceberg-spec Avro files.

**Manifest List** (`snap-<snapshotId>.avro`):
- Avro OCF file with Iceberg manifest list schema
- Each entry: `{ manifest_path, manifest_length, partition_spec_id: 0, content: 0|1, sequence_number, added_snapshot_id, added_files_count, added_rows_count, ... }`
- One entry for data manifest (`content=0`), optionally one for delete manifest (`content=1`)
- Must include Avro metadata headers: `format-version: "2"`, `avro.schema`

**Manifest File** (`m<N>.avro`):
- Avro OCF with Iceberg manifest entry schema
- Each entry represents a data file: `{ status: 1 (ADDED), snapshot_id, data_file: { content: 0, file_path, file_format: "PARQUET", record_count, file_size_in_bytes, column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds, ... } }`
- File paths: resolve relative DuckLake paths with `data_path` prefix (e.g., `s3://<DUCKLAKE_DATA_BUCKET>/ducklake-XXX.parquet`)
- Statistics encoding: DuckLake stores min/max as text → encode per Iceberg binary spec (long→8-byte LE, double→8-byte LE, string→UTF-8, timestamp→8-byte LE microseconds)
- Must include Avro metadata headers: `schema` (JSON of Iceberg table schema), `schema-id`, `partition-spec`, `partition-spec-id`, `format-version: "2"`, `content: "data"` or `content: "deletes"`

**Important**: The Avro schemas must match exactly what DuckDB's Iceberg extension expects. We'll need to match the field names and types from the Iceberg v2 spec. The captured real Avro files from mitmproxy can be used as reference to validate.

**Approach for manifest generation**: Generate both manifest files first, then generate the manifest list (which needs manifest sizes). Cache results keyed by `(tableId, snapshotId)` since DuckDB calls loadTable 4 times.

### 6. S3 Virtual File Intercept

**Modify `src/http/routes/s3/commands/get-object.ts`**:

In the iceberg-typed GET handler, before calling `s3Protocol.getObject()`, check if the key contains the virtual prefix (`__ducklake__/`):

```typescript
// In the iceberg-type handler:
const key = req.Params['*']
if (key && isDuckLakeVirtualPath(key)) {
  const generator = new DuckLakeVirtualFileGenerator(ctx.req.db)
  const buffer = await generator.generate(key)
  return {
    statusCode: 200,
    headers: {
      'content-type': 'application/octet-stream',
      'content-length': buffer.length.toString(),
      'etag': `"${md5(buffer)}"`,
    },
    responseBody: buffer,
  }
}
// ... existing getObject code
```

**Modify `src/http/routes/s3/commands/head-object.ts`**:

Same pattern — intercept virtual paths and return size/headers without generating the full file (or generate and cache it).

### 7. Plugin Wiring (`src/http/plugins/iceberg.ts`)

Modify `icebergRestCatalog` plugin:

```typescript
if (icebergCatalogMode === 'ducklake') {
  req.icebergCatalog = new DuckLakeCatalog({
    db: req.db.pool.acquire(),  // knex instance
    ducklakeSchema: ducklakeSchema,
    virtualPrefix: ducklakeVirtualPrefix,
    dataBucket: ducklakeDataBucket,       // customer's Supabase storage bucket
    warehouseName: analyticsBucketName,   // the customer's analytics bucket name (used as catalog prefix)
  })
} else {
  req.icebergCatalog = new TenantAwareRestCatalog({ ... })  // existing
}
```

Note: In DuckLake mode there is only one catalog per customer — the "warehouse" name is derived from the analytics bucket the customer creates. There is no multi-catalog support; all metadata lives in the customer's DB.

Change `FastifyRequest.icebergCatalog` type from `TenantAwareRestCatalog` to a union/interface type that covers both.

Modify `detectS3IcebergBucket` plugin — for DuckLake mode, the bucket detection needs to handle the customer's data bucket (`DUCKLAKE_DATA_BUCKET`). The DuckLake data files live at `s3://<DUCKLAKE_DATA_BUCKET>/ducklake-XXX.parquet`, so the S3 bucket from DuckDB's perspective needs to resolve to the customer's actual Supabase storage bucket.

### 8. Key Mapping Details

**S3 Bucket**: The table location bucket is the customer's Supabase storage bucket (configured via `DUCKLAKE_DATA_BUCKET`, default `data-lake`). Table location = `s3://<DUCKLAKE_DATA_BUCKET>/`, virtual metadata at `s3://<DUCKLAKE_DATA_BUCKET>/__ducklake__/...`, real data files at `s3://<DUCKLAKE_DATA_BUCKET>/ducklake-XXX.parquet`. The customer specifies this bucket during setup — it's the Supabase storage bucket where DuckLake stores its parquet data files.

**Bucket Detection**: In DuckLake mode, `detectS3IcebergBucket` needs to recognize the customer's data bucket as an iceberg-like bucket (it doesn't have the `--iceberg` suffix). Add a DuckLake-specific detection path: if `ICEBERG_CATALOG_MODE=ducklake`, match the bucket name against `DUCKLAKE_DATA_BUCKET` and set `isIcebergBucket = true` + `internalIcebergBucketName = <DUCKLAKE_DATA_BUCKET>`. This enables the iceberg-type S3 route handlers which then handle virtual file interception.

**Data file paths**: DuckLake stores relative paths (`ducklake-XXX.parquet`) with `path_is_relative=true`. Resolve to absolute by prepending `ducklake_metadata.data_path` (`s3://<DUCKLAKE_DATA_BUCKET>/`). In manifests, use the full path `s3://<DUCKLAKE_DATA_BUCKET>/ducklake-XXX.parquet`.

**Iceberg field IDs**: Use `column_id` from `ducklake_column` (starts at 1, 1-based).

**Snapshot IDs**: DuckLake uses small integers (0, 1, 2...). Iceberg expects 64-bit longs. Use them directly — DuckDB doesn't care as long as they're consistent.

**Avro approach**: Build Avro schemas from the Iceberg v2 spec. The key specs are in the [Iceberg table spec](https://iceberg.apache.org/spec/#manifests) for manifest list and manifest file Avro schemas. Iterate by testing against DuckDB.

### 9. New Dependency

Add `avsc` to `package.json` for Avro serialization.

### 10. Files Summary

**New files:**
- `src/storage/protocols/iceberg/catalog/ducklake-catalog.ts` — main catalog implementation
- `src/storage/protocols/iceberg/catalog/ducklake-queries.ts` — SQL queries
- `src/storage/protocols/iceberg/catalog/ducklake-types.ts` — type mapping
- `src/storage/protocols/iceberg/catalog/ducklake-avro.ts` — Avro manifest generation

**Modified files:**
- `src/config.ts` — add ICEBERG_CATALOG_MODE, DUCKLAKE_SCHEMA, DUCKLAKE_VIRTUAL_PREFIX, DUCKLAKE_DATA_BUCKET
- `src/http/plugins/iceberg.ts` — conditional catalog instantiation + bucket detection for DuckLake
- `src/http/routes/s3/commands/get-object.ts` — virtual file intercept
- `src/http/routes/s3/commands/head-object.ts` — virtual file intercept
- `src/storage/protocols/iceberg/catalog/index.ts` — re-export new modules
- `package.json` — add `avsc` dependency

**No changes needed:**
- Route handlers (`table.ts`, `namespace.ts`, `catalog.ts`) — they already call `req.icebergCatalog.*` generically
- S3 router infrastructure — type-based routing already works
- Data file serving — parquet files are real in S3

### 11. Verification Plan

1. Set env: `ICEBERG_CATALOG_MODE=ducklake`, `DUCKLAKE_SCHEMA=public`, `ICEBERG_WAREHOUSE=warehouse`
2. Start storage API
3. Run the DuckDB query script at `~/code/sandbox/iceberg/examples/analytics_buckets/query.py` (modified to point to the DuckLake warehouse)
4. Verify: config request returns valid JSON, namespace loads, table loads with correct schema/snapshots, manifest list Avro parses correctly, manifest file Avro parses correctly, parquet data files serve correctly, query returns correct data
5. Use mitmproxy to compare request/response against the captured baseline in `~/Downloads/requests-raw.txt`
