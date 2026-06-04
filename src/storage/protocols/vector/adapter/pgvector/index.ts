import { createHash, randomUUID } from 'node:crypto'
import {
  CreateIndexCommandInput,
  CreateIndexCommandOutput,
  DeleteIndexCommandInput,
  DeleteIndexCommandOutput,
  DeleteVectorsInput,
  DeleteVectorsOutput,
  DistanceMetric,
  GetVectorsCommandInput,
  GetVectorsCommandOutput,
  ListVectorsInput,
  ListVectorsOutput,
  PutVectorsInput,
  PutVectorsOutput,
  QueryVectorsInput,
  QueryVectorsOutput,
} from '@aws-sdk/client-s3vectors'
import {
  PgExecutor,
  PgTransaction,
  PgTransactionalExecutor,
  quoteIdentifier,
} from '@internal/database'
import { ERRORS } from '@internal/errors'
import BaseTtlCache from '@isaacs/ttlcache'
import type { DocumentType } from '@smithy/types'
import {
  MAX_DELETE_VECTOR_KEYS,
  MAX_GET_VECTOR_KEYS,
  MAX_LIST_RESULTS,
  MAX_QUERY_TOP_K,
  MAX_SEGMENT_COUNT,
  validatePutVectors,
  validateVectorKeys,
} from '../../limits'
import { paginateNPlusOne } from '../../pagination'
import { VectorStore } from '../s3-vector'
import { handlePgVectorError } from './errors'
import { S3VectorFilter, translateFilter } from './filter'

// Cache the resolved distance metric for ~5 min per (bucket, index). Avoids
// hammering pg_index on every queryVectors call; stale entries (e.g., an
// index dropped out-of-band) self-heal on miss via the lookupMetric fallback.
//
// Module-scoped on purpose: in multi-tenant mode the Fastify plugin builds a
// new PgVectorStore per request, so an instance-scoped cache would be cold
// every call. Bucket/index identifiers are already tenant-scoped at the HTTP
// layer (each tenant's `storage_vectors` schema lives in its own DB pool), so
// sharing across requests within a process is safe.
const METRIC_CACHE_TTL_MS = 5 * 60 * 1000
const METRIC_CACHE_MAX = 1_000
const metricCache = new BaseTtlCache<string, DistanceMetric>({
  ttl: METRIC_CACHE_TTL_MS,
  max: METRIC_CACHE_MAX,
  updateAgeOnGet: true,
})
type PgVectorTableCapabilityKind = 'bridged-hnsw' | 'standard' | 'unknown'
interface PgVectorTableCapability {
  kind: PgVectorTableCapabilityKind
  requiresManualUpsert: boolean
  requiresExactQueryScan: boolean
}
const STANDARD_TABLE_CAPABILITY: PgVectorTableCapability = {
  kind: 'standard',
  requiresManualUpsert: false,
  requiresExactQueryScan: false,
}
const BRIDGED_HNSW_TABLE_CAPABILITY: PgVectorTableCapability = {
  kind: 'bridged-hnsw',
  requiresManualUpsert: true,
  requiresExactQueryScan: true,
}
const UNKNOWN_TABLE_CAPABILITY: PgVectorTableCapability = {
  kind: 'unknown',
  requiresManualUpsert: false,
  requiresExactQueryScan: true,
}
const tableCapabilityCaches = new WeakMap<object, Map<string, Promise<PgVectorTableCapability>>>()

function metricCacheKey(bucket: string, index: string): string {
  return `${bucket}\x00${index}`
}

const SCHEMA = 'storage_vectors'
const MAX_DIMENSIONS = 4_000
const DEFAULT_HNSW_EF_SEARCH = 40

// Manual OrioleDB upserts can still deadlock under concurrent writers.
const MANUAL_UPSERT_MAX_ATTEMPTS = 3

function validatePositiveInt(name: string, value: number, max: number): number {
  if (!Number.isInteger(value) || value < 1 || value > max) {
    throw ERRORS.InvalidParameter(name, {
      message: `${name} must be an integer in [1, ${max}], got: ${value}`,
    })
  }
  return value
}

function isRetryableWriteConflict(error: unknown): boolean {
  const code = (error as { code?: unknown })?.code
  return code === '40P01' || code === '40001'
}

function isBridgedHnswUpsertError(error: unknown): boolean {
  const parts = [
    (error as { message?: unknown })?.message,
    (error as { detail?: unknown })?.detail,
  ].filter((part): part is string => typeof part === 'string')

  return parts.some((part) => part.includes('unexpected self-updated tuple'))
}

function validateListSegment(input: ListVectorsInput):
  | {
      segmentCount: number
      segmentIndex: number
    }
  | undefined {
  const hasSegmentCount = input.segmentCount !== undefined
  const hasSegmentIndex = input.segmentIndex !== undefined

  if (hasSegmentCount !== hasSegmentIndex) {
    throw ERRORS.InvalidParameter('segmentCount/segmentIndex', {
      message: 'segmentCount and segmentIndex must be provided together',
    })
  }

  if (!hasSegmentCount) {
    return undefined
  }

  const segmentCount = validatePositiveInt('segmentCount', input.segmentCount!, MAX_SEGMENT_COUNT)
  const segmentIndex = input.segmentIndex!
  if (!Number.isInteger(segmentIndex) || segmentIndex < 0 || segmentIndex >= segmentCount) {
    throw ERRORS.InvalidParameter('segmentIndex', {
      message: `segmentIndex must be an integer in [0, ${segmentCount - 1}], got: ${segmentIndex}`,
    })
  }

  return { segmentCount, segmentIndex }
}

function tableCapabilityCache(db: object): Map<string, Promise<PgVectorTableCapability>> {
  let cache = tableCapabilityCaches.get(db)
  if (!cache) {
    cache = new Map()
    tableCapabilityCaches.set(db, cache)
  }
  return cache
}

function capabilityForAccessMethod(accessMethod: unknown): PgVectorTableCapability {
  if (typeof accessMethod !== 'string') {
    return UNKNOWN_TABLE_CAPABILITY
  }

  return accessMethod === 'orioledb' ? BRIDGED_HNSW_TABLE_CAPABILITY : STANDARD_TABLE_CAPABILITY
}

function forgetTableCapability(db: object, table: string): void {
  tableCapabilityCaches.get(db)?.delete(table)
}

async function resolveTableCapability(
  db: PgExecutor,
  table: string
): Promise<PgVectorTableCapability> {
  const cache = tableCapabilityCache(db)
  let capability = cache.get(table)
  if (capability) {
    return capability
  }

  const capabilityProbe: Promise<PgVectorTableCapability> = db
    .query<{ amname: string | null }>({
      text: `
        SELECT am.amname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_am am ON am.oid = c.relam
        WHERE n.nspname = $1
          AND c.relname = $2
        LIMIT 1
      `,
      values: [SCHEMA, table],
    })
    .then((result: { rows?: Array<Record<string, unknown>> }) => {
      if (result.rows?.[0]?.amname === undefined && cache.get(table) === capabilityProbe) {
        cache.delete(table)
      }
      return capabilityForAccessMethod(result.rows?.[0]?.amname)
    })
    .catch(() => {
      // Do not cache probe failures. Query paths treat unknown tables as
      // exact-scan for correctness, and the next request can retry.
      if (cache.get(table) === capabilityProbe) {
        cache.delete(table)
      }
      return UNKNOWN_TABLE_CAPABILITY
    })
  cache.set(table, capabilityProbe)

  return capabilityProbe
}

export type PgExecutorResolver =
  | PgTransactionalExecutor
  | PgTransaction
  | {
      resolve: () => PgTransactionalExecutor | PgTransaction
      root?: () => PgTransactionalExecutor | PgTransaction
    }

function isPgExecutorProvider(r: PgExecutorResolver): r is {
  resolve: () => PgTransactionalExecutor | PgTransaction
  root?: () => PgTransactionalExecutor | PgTransaction
} {
  return typeof (r as { resolve?: unknown }).resolve === 'function'
}

function resolvePgExecutor(r: PgExecutorResolver): PgTransactionalExecutor | PgTransaction {
  return isPgExecutorProvider(r) ? r.resolve() : r
}

function resolveRootPgExecutor(r: PgExecutorResolver): PgTransactionalExecutor | PgTransaction {
  return isPgExecutorProvider(r) ? (r.root?.() ?? r.resolve()) : r
}

function hasRootPgResolver(r: PgExecutorResolver): boolean {
  return isPgExecutorProvider(r) && typeof r.root === 'function'
}

async function withPgTransaction<T>(
  db: PgTransactionalExecutor | PgTransaction,
  fn: (trx: PgTransaction) => Promise<T>
): Promise<T> {
  if (db instanceof PgTransaction) {
    const savepoint = nextSavepointName()
    await db.query(`SAVEPOINT ${savepoint}`)
    try {
      const result = await fn(db)
      await db.query(`RELEASE SAVEPOINT ${savepoint}`)
      return result
    } catch (error) {
      await db.query(`ROLLBACK TO SAVEPOINT ${savepoint}`).catch(() => undefined)
      await db.query(`RELEASE SAVEPOINT ${savepoint}`).catch(() => undefined)
      throw error
    }
  }

  const trx = await db.beginTransaction()
  try {
    const result = await fn(trx)
    await trx.commit()
    return result
  } catch (error) {
    await trx.rollback().catch(() => undefined)
    throw error
  }
}

function tableName(vectorBucketName: string, indexName: string): string {
  // Combined logical key may exceed Postgres' 63-char identifier limit, so we
  // hash. SHA-256 truncated to 24 hex chars keeps the table name well within
  // the limit and unique enough for any reasonable dev dataset.
  const hash = createHash('sha256')
    .update(`${vectorBucketName}\x00${indexName}`)
    .digest('hex')
    .slice(0, 24)
  return `vector_${hash}`
}

function qualifiedTable(vectorBucketName: string, indexName: string): string {
  return qualifiedTableName(tableName(vectorBucketName, indexName))
}

function qualifiedTableName(table: string): string {
  return `${quoteIdentifier(SCHEMA)}.${quoteIdentifier(table)}`
}

function nextSavepointName(): string {
  return quoteIdentifier(`pgvector_store_${randomUUID().replace(/-/g, '_')}`)
}

function offsetPlaceholders(sql: string, offset: number): string {
  if (offset === 0) {
    return sql
  }

  return sql.replace(/\$(\d+)/g, (_, index) => `$${Number(index) + offset}`)
}

interface OpClassChoice {
  opClass: 'halfvec_cosine_ops' | 'halfvec_l2_ops'
  distanceOp: '<=>' | '<->'
  metric: DistanceMetric
}

function distanceChoice(metric: string | undefined): OpClassChoice {
  switch (metric) {
    case 'cosine':
      return { opClass: 'halfvec_cosine_ops', distanceOp: '<=>', metric: 'cosine' }
    case 'euclidean':
      return { opClass: 'halfvec_l2_ops', distanceOp: '<->', metric: 'euclidean' }
    default:
      throw ERRORS.InvalidParameter('distanceMetric', {
        message: `Unsupported distance metric for pgvector backend: ${metric}`,
      })
  }
}

function toVectorLiteral(values: number[]): string {
  return `[${values.join(',')}]`
}

type PgVectorMetadataPrimitive = string | boolean | number
type PgVectorMetadata = Record<string, PgVectorMetadataPrimitive | PgVectorMetadataPrimitive[]>

function validateMetadata(metadata: DocumentType | undefined, vectorKey: string): PgVectorMetadata {
  if (metadata === undefined) {
    return {}
  }

  const invalidMetadata = (message: string) =>
    ERRORS.InvalidParameter('vectors.metadata', {
      message: `Invalid metadata for vector "${vectorKey}": ${message}`,
    })

  if (metadata === null || Array.isArray(metadata) || typeof metadata !== 'object') {
    throw invalidMetadata('metadata must be an object')
  }

  const validatePrimitive = (value: unknown, fieldName: string): PgVectorMetadataPrimitive => {
    if (typeof value === 'string' || typeof value === 'boolean') {
      return value
    }

    if (typeof value === 'number' && Number.isFinite(value)) {
      return value
    }

    throw invalidMetadata(
      `metadata field "${fieldName}" must be a string, boolean, or finite number`
    )
  }

  const validated: PgVectorMetadata = {}
  for (const [key, value] of Object.entries(metadata)) {
    if (Array.isArray(value)) {
      validated[key] = value.map((item, index) => validatePrimitive(item, `${key}[${index}]`))
      continue
    }

    validated[key] = validatePrimitive(value, key)
  }

  return validated
}

export class PgVectorStore implements VectorStore {
  readonly maxDimensions = MAX_DIMENSIONS
  readonly transactionalIndexOperations: boolean

  constructor(private readonly executor: PgExecutorResolver) {
    this.transactionalIndexOperations = hasRootPgResolver(executor)
  }

  private db(): PgTransactionalExecutor | PgTransaction {
    return resolvePgExecutor(this.executor)
  }

  private rootDb(): PgTransactionalExecutor | PgTransaction {
    return resolveRootPgExecutor(this.executor)
  }

  async createVectorIndex(command: CreateIndexCommandInput): Promise<CreateIndexCommandOutput> {
    if (!command.indexName || !command.vectorBucketName) {
      throw ERRORS.MissingParameter('indexName/vectorBucketName')
    }
    if (command.dataType !== 'float32') {
      throw ERRORS.InvalidParameter('dataType', {
        message: `Unsupported data type for pgvector backend: ${command.dataType}`,
      })
    }
    const dimension = command.dimension
    // pgvector's HNSW halfvec operator classes (halfvec_cosine_ops,
    // halfvec_l2_ops) cap out at 4000 dimensions, which covers the most
    // common production embedding models (incl. OpenAI text-embedding-3-large
    // at 3072) and S3Vectors' own 4096 cap after trivial truncation. We
    // validate at create-index time so we fail loudly with a clear error
    // rather than later at INDEX-build time with an opaque pgvector error.
    if (!dimension || !Number.isInteger(dimension) || dimension < 1 || dimension > MAX_DIMENSIONS) {
      throw ERRORS.InvalidParameter('dimension', {
        message: `Invalid dimension for pgvector HNSW: ${dimension} (must be 1..${MAX_DIMENSIONS})`,
      })
    }

    const choice = distanceChoice(command.distanceMetric)
    const table = tableName(command.vectorBucketName, command.indexName)

    return handlePgVectorError(
      async () => {
        const db = this.db()
        // Wrap DDL in a transaction so a failed CREATE INDEX (missing
        // opclass, permissions, transient error) rolls back the CREATE TABLE.
        // Otherwise the orphan table would block retries with "already exists".
        await withPgTransaction(db, async (trx) => {
          // Postgres doesn't allow parameter binding inside type modifiers
          // like `halfvec(N)` — N must be a literal at parse time. We've
          // validated `dimension` is an integer in [1, 4_000] above, so
          // inlining is safe. halfvec stores each dimension as a 2-byte
          // float16; recall loss vs float32 is typically <0.5% on normalized
          // embeddings and index size/memory drop ~50%.
          await trx.query(`
            CREATE TABLE ${qualifiedTableName(table)}
            (
              key text PRIMARY KEY,
              embedding halfvec(${dimension}) NOT NULL,
              metadata jsonb NOT NULL DEFAULT '{}'::jsonb
            )
          `)
          await trx.query(`
            CREATE INDEX ${quoteIdentifier(`${table}_hnsw`)}
            ON ${qualifiedTableName(table)}
            USING hnsw (embedding ${choice.opClass})
          `)
        })
        forgetTableCapability(db, table)
        forgetTableCapability(this.rootDb(), table)
        // Prime the metric cache so subsequent queryVectors don't need a
        // round-trip lookup to pick the right distance operator. The two
        // names are validated non-empty at the top of this method.
        metricCache.set(
          metricCacheKey(command.vectorBucketName as string, command.indexName as string),
          choice.metric
        )
        return { $metadata: {} } as CreateIndexCommandOutput
      },
      { type: 'vector-index', name: command.indexName }
    )
  }

  async deleteVectorIndex(param: DeleteIndexCommandInput): Promise<DeleteIndexCommandOutput> {
    const bucket = param.vectorBucketName!
    const index = param.indexName!
    const table = tableName(bucket, index)
    return handlePgVectorError(
      async () => {
        const db = this.db()
        await db.query(`DROP TABLE IF EXISTS ${qualifiedTableName(table)}`)
        forgetTableCapability(db, table)
        forgetTableCapability(this.rootDb(), table)
        metricCache.delete(metricCacheKey(bucket, index))
        return { $metadata: {} } as DeleteIndexCommandOutput
      },
      { type: 'vector-index', name: index }
    )
  }

  async putVectors(command: PutVectorsInput): Promise<PutVectorsOutput> {
    const bucket = command.vectorBucketName!
    const index = command.indexName!
    const vectors = validatePutVectors(command.vectors)

    return handlePgVectorError(
      async () => {
        const db = this.db()

        const rows = vectors.map((v) => {
          if (!v.key) throw ERRORS.MissingParameter('vector.key')
          if (!v.data || !v.data.float32) throw ERRORS.MissingParameter('vector.data.float32')
          return {
            key: v.key,
            embedding: toVectorLiteral(v.data.float32 as number[]),
            // Pass the object directly. `JSON.stringify(rows)` below serializes
            // it as a JSON object so jsonb_to_recordset parses `metadata` as a
            // JSONB object (not a JSONB string). Otherwise `metadata->>'key'`
            // returns NULL because the column would hold a quoted string.
            metadata: validateMetadata(v.metadata, v.key),
          }
        })
        const serializedRows = JSON.stringify([...rows].sort((a, b) => a.key.localeCompare(b.key)))
        const table = tableName(bucket, index)
        const qualified = qualifiedTableName(table)
        const capability = await resolveTableCapability(db, table)

        if (capability.requiresManualUpsert) {
          await this.putVectorsManually(db, qualified, serializedRows)
          return {} as PutVectorsOutput
        }

        try {
          await db.query({
            text: `
              INSERT INTO ${qualified} (key, embedding, metadata)
              SELECT key, embedding::halfvec, metadata
              FROM jsonb_to_recordset($1::jsonb)
                AS x(key text, embedding text, metadata jsonb)
              ON CONFLICT (key) DO UPDATE
                SET embedding = EXCLUDED.embedding,
                    metadata  = EXCLUDED.metadata
            `,
            values: [serializedRows],
          })
        } catch (e) {
          if (!isBridgedHnswUpsertError(e)) {
            throw e
          }

          forgetTableCapability(db, table)
          const refreshedCapability = await resolveTableCapability(db, table)
          if (!refreshedCapability.requiresManualUpsert) {
            throw e
          }

          await this.putVectorsManually(db, qualified, serializedRows)
        }

        return {} as PutVectorsOutput
      },
      { type: 'vectors', name: index }
    )
  }

  private async putVectorsManually(
    db: PgTransactionalExecutor | PgTransaction,
    table: string,
    serializedRows: string
  ): Promise<void> {
    // OrioleDB supports pgvector HNSW indexes through index bridging, but its
    // bridged HNSW path can reject ON CONFLICT DO UPDATE with "unexpected
    // self-updated tuple". Plain UPDATE, INSERT, and DO NOTHING work, so this
    // fallback preserves upsert semantics without that conflict action.
    for (let attempt = 1; attempt <= MANUAL_UPSERT_MAX_ATTEMPTS; attempt += 1) {
      try {
        await withPgTransaction(db, async (trx) => {
          await trx.query({
            text: `
              WITH input AS (
                SELECT key, embedding::halfvec AS embedding, metadata
                FROM jsonb_to_recordset($1::jsonb)
                  AS x(key text, embedding text, metadata jsonb)
              )
              UPDATE ${table} AS target
                 SET embedding = input.embedding,
                     metadata = input.metadata
                FROM input
               WHERE target.key = input.key
            `,
            values: [serializedRows],
          })

          await trx.query({
            text: `
              INSERT INTO ${table} (key, embedding, metadata)
              SELECT key, embedding::halfvec, metadata
                FROM jsonb_to_recordset($1::jsonb)
                  AS x(key text, embedding text, metadata jsonb)
              ON CONFLICT (key) DO NOTHING
            `,
            values: [serializedRows],
          })

          // Close the READ COMMITTED race where two writers both miss the first
          // UPDATE for a new key, one INSERT wins, and the other INSERT does
          // nothing. The final UPDATE applies the later writer's payload without
          // using ON CONFLICT DO UPDATE, which OrioleDB bridged HNSW can reject.
          await trx.query({
            text: `
              WITH input AS (
                SELECT key, embedding::halfvec AS embedding, metadata
                FROM jsonb_to_recordset($1::jsonb)
                  AS x(key text, embedding text, metadata jsonb)
              )
              UPDATE ${table} AS target
                 SET embedding = input.embedding,
                     metadata = input.metadata
                FROM input
               WHERE target.key = input.key
            `,
            values: [serializedRows],
          })
        })
        return
      } catch (error) {
        if (attempt === MANUAL_UPSERT_MAX_ATTEMPTS || !isRetryableWriteConflict(error)) {
          throw error
        }
      }
    }
  }

  private async queryVectorsRaw(
    db: PgTransactionalExecutor | PgTransaction,
    table: string,
    sql: string,
    params: unknown[],
    topK: number
  ): Promise<{ rows: unknown[] }> {
    const capability = await resolveTableCapability(db, table)
    if (!capability.requiresExactQueryScan) {
      return withPgTransaction(db, async (trx): Promise<{ rows: unknown[] }> => {
        await trx.query({
          text: `SELECT set_config('hnsw.ef_search', $1, true)`,
          values: [String(Math.max(topK, DEFAULT_HNSW_EF_SEARCH))],
        })
        return trx.query({ text: sql, values: params })
      })
    }

    // The same OrioleDB bridged HNSW path that rejects ON CONFLICT DO UPDATE
    // can also miss rows inserted after the index was created. Use exact scan
    // semantics for pools where we have observed that path.
    return withPgTransaction(db, async (trx): Promise<{ rows: unknown[] }> => {
      await trx.query(`
        SELECT set_config('enable_indexscan', 'off', true),
               set_config('enable_bitmapscan', 'off', true)
      `)
      return trx.query({ text: sql, values: params })
    })
  }

  async getVectors(input: GetVectorsCommandInput): Promise<GetVectorsCommandOutput> {
    const bucket = input.vectorBucketName!
    const index = input.indexName!
    const keys = validateVectorKeys(input.keys, MAX_GET_VECTOR_KEYS)

    const wantData = input.returnData === true
    const wantMeta = input.returnMetadata === true

    return handlePgVectorError(
      async () => {
        const cols = ['key']
        if (wantData) cols.push('embedding::text AS embedding')
        if (wantMeta) cols.push('metadata')
        const sql = `SELECT ${cols.join(', ')} FROM ${qualifiedTable(bucket, index)} WHERE key = ANY($1::text[])`
        const result = await this.db().query({ text: sql, values: [keys] })
        const rows = result.rows as Array<{
          key: string
          embedding?: string
          metadata?: DocumentType
        }>
        return {
          vectors: rows.map((r) => ({
            key: r.key,
            data:
              wantData && r.embedding ? { float32: parseVectorLiteral(r.embedding) } : undefined,
            metadata: wantMeta ? (r.metadata ?? {}) : undefined,
          })),
        } as GetVectorsCommandOutput
      },
      { type: 'vectors', name: index }
    )
  }

  async deleteVectors(input: DeleteVectorsInput): Promise<DeleteVectorsOutput> {
    const bucket = input.vectorBucketName!
    const index = input.indexName!
    const keys = validateVectorKeys(input.keys, MAX_DELETE_VECTOR_KEYS)

    return handlePgVectorError(
      async () => {
        await this.db().query({
          text: `DELETE FROM ${qualifiedTable(bucket, index)} WHERE key = ANY($1::text[])`,
          values: [keys],
        })
        return {} as DeleteVectorsOutput
      },
      { type: 'vectors', name: index }
    )
  }

  async queryVectors(input: QueryVectorsInput): Promise<QueryVectorsOutput> {
    const bucket = input.vectorBucketName!
    const index = input.indexName!
    const queryVector = input.queryVector
    if (!queryVector || !queryVector.float32) {
      throw ERRORS.MissingParameter('queryVector.float32')
    }

    const wantMeta = input.returnMetadata === true
    const wantDistance = input.returnDistance === true
    const topK = validatePositiveInt('topK', input.topK ?? 10, MAX_QUERY_TOP_K)

    return handlePgVectorError(
      async () => {
        // The operator chosen for the distance expression AND for ORDER BY must
        // match the HNSW index's operator class — otherwise the index isn't
        // used and the returned distance is in the wrong metric. Resolve the
        // metric (cached after createVectorIndex) before assembling the query.
        const metric = await this.getOrLookupMetric(bucket, index)
        const distanceOp: '<=>' | '<->' = metric === 'euclidean' ? '<->' : '<=>'

        const cols: string[] = ['key']
        if (wantDistance) cols.push(`embedding ${distanceOp} $1::halfvec AS distance`)
        if (wantMeta) cols.push('metadata')

        const params: unknown[] = []
        if (wantDistance) params.push(toVectorLiteral(queryVector.float32 as number[]))

        let whereClause = ''
        if (input.filter) {
          const translated = translateFilter(input.filter as unknown as S3VectorFilter)
          whereClause = ' WHERE ' + offsetPlaceholders(translated.sql, params.length)
          params.push(...translated.params)
        }

        params.push(toVectorLiteral(queryVector.float32 as number[]))
        params.push(topK)

        const table = tableName(bucket, index)
        const orderVectorParam = params.length - 1
        const limitParam = params.length
        const sql = `SELECT ${cols.join(', ')}
                     FROM ${qualifiedTableName(table)}${whereClause}
                     ORDER BY embedding ${distanceOp} $${orderVectorParam}::halfvec ASC
                     LIMIT $${limitParam}`
        const result = await this.queryVectorsRaw(this.db(), table, sql, params, topK)
        const rows = result.rows as Array<{
          key: string
          distance?: number
          metadata?: DocumentType
        }>

        return {
          vectors: rows.map((r) => ({
            key: r.key,
            distance: wantDistance ? r.distance : undefined,
            metadata: wantMeta ? (r.metadata ?? {}) : undefined,
          })),
          distanceMetric: metric,
        } as QueryVectorsOutput
      },
      { type: 'vector-index', name: index }
    )
  }

  private async getOrLookupMetric(bucket: string, index: string): Promise<DistanceMetric> {
    const key = metricCacheKey(bucket, index)
    const cached = metricCache.get(key)
    if (cached) return cached
    const metric = await this.lookupMetric(bucket, index)
    metricCache.set(key, metric)
    return metric
  }

  async listVectors(input: ListVectorsInput): Promise<ListVectorsOutput> {
    const bucket = input.vectorBucketName!
    const index = input.indexName!
    const wantData = input.returnData === true
    const wantMeta = input.returnMetadata === true
    const maxResults = validatePositiveInt('maxResults', input.maxResults ?? 500, MAX_LIST_RESULTS)
    const cursor = input.nextToken
    const segment = validateListSegment(input)

    return handlePgVectorError(
      async () => {
        const cols = ['key']
        if (wantData) cols.push('embedding::text AS embedding')
        if (wantMeta) cols.push('metadata')

        const params: unknown[] = []
        const whereClauses: string[] = []
        if (cursor) {
          whereClauses.push(`key > $${params.length + 1}`)
          params.push(cursor)
        }
        if (segment) {
          whereClauses.push(
            `mod(abs(hashtext(key)::bigint), $${params.length + 1}::bigint) = $${params.length + 2}::bigint`
          )
          params.push(segment.segmentCount, segment.segmentIndex)
        }
        params.push(maxResults + 1)

        const whereClause = whereClauses.length > 0 ? ` WHERE ${whereClauses.join(' AND ')}` : ''
        const sql = `SELECT ${cols.join(', ')}
                     FROM ${qualifiedTable(bucket, index)}${whereClause}
                     ORDER BY key ASC
                     LIMIT $${params.length}`
        const result = await this.db().query({ text: sql, values: params })
        const rows = result.rows as Array<{
          key: string
          embedding?: string
          metadata?: DocumentType
        }>
        const { pageRows, nextToken } = paginateNPlusOne(rows, maxResults, (row) => row.key)
        return {
          vectors: pageRows.map((r) => ({
            key: r.key,
            data:
              wantData && r.embedding ? { float32: parseVectorLiteral(r.embedding) } : undefined,
            metadata: wantMeta ? (r.metadata ?? {}) : undefined,
          })),
          nextToken,
        } as ListVectorsOutput
      },
      { type: 'vector-index', name: index }
    )
  }

  private async lookupMetric(bucket: string, index: string): Promise<DistanceMetric> {
    const table = tableName(bucket, index)
    try {
      const result = await this.db().query({
        text: `
          SELECT am.amname, opc.opcname
          FROM pg_index i
          JOIN pg_class ic ON ic.oid = i.indexrelid
          JOIN pg_class tc ON tc.oid = i.indrelid
          JOIN pg_namespace n ON n.oid = tc.relnamespace
          JOIN pg_am am ON am.oid = ic.relam
          JOIN pg_opclass opc ON opc.oid = ANY(i.indclass)
          WHERE n.nspname = $1
            AND tc.relname = $2
            AND am.amname = 'hnsw'
          LIMIT 1
        `,
        values: [SCHEMA, table],
      })
      const op = result.rows?.[0]?.opcname as string | undefined
      if (op === 'halfvec_l2_ops') return 'euclidean'
      return 'cosine'
    } catch {
      return 'cosine'
    }
  }
}

function parseVectorLiteral(literal: string): number[] {
  // pgvector returns vectors as e.g. "[1,2,3]"
  if (!literal.startsWith('[') || !literal.endsWith(']')) return []
  return literal
    .slice(1, -1)
    .split(',')
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n))
}
