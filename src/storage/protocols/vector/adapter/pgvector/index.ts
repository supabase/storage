import { createHash } from 'node:crypto'
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
import { ERRORS } from '@internal/errors'
import BaseTtlCache from '@isaacs/ttlcache'
import type { DocumentType } from '@smithy/types'
import type { Knex } from 'knex'
import { VectorStore } from '../s3-vector'
import { handlePgVectorError } from './errors'
import { S3VectorFilter, translateFilter } from './filter'

// Cache the resolved distance metric for ~5 min per (bucket, index). Avoids
// hammering pg_index on every queryVectors call; stale entries (e.g., an
// index dropped out-of-band) self-heal on miss via the lookupMetric fallback.
const METRIC_CACHE_TTL_MS = 5 * 60 * 1000
const METRIC_CACHE_MAX = 1_000

const SCHEMA = 'storage_vectors'

/**
 * Knex handle abstraction: ST mode passes a singleton Knex; MT mode passes
 * a function that resolves to the tenant's pool on each call. We can't use a
 * bare `() => Knex` because `Knex` itself is callable, so the function form is
 * wrapped in a tagged object.
 */
export type KnexResolver = Knex | { resolve: () => Knex }

function resolveKnex(r: KnexResolver): Knex {
  return 'resolve' in r && typeof r.resolve === 'function' ? r.resolve() : (r as Knex)
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
  return `${SCHEMA}.${tableName(vectorBucketName, indexName)}`
}

interface OpClassChoice {
  opClass: 'vector_cosine_ops' | 'vector_l2_ops'
  distanceOp: '<=>' | '<->'
  metric: DistanceMetric
}

function distanceChoice(metric: string | undefined): OpClassChoice {
  switch (metric) {
    case 'cosine':
      return { opClass: 'vector_cosine_ops', distanceOp: '<=>', metric: 'cosine' }
    case 'euclidean':
      return { opClass: 'vector_l2_ops', distanceOp: '<->', metric: 'euclidean' }
    default:
      throw ERRORS.InvalidParameter('distanceMetric', {
        message: `Unsupported distance metric for pgvector backend: ${metric}`,
      })
  }
}

function toVectorLiteral(values: number[]): string {
  return `[${values.join(',')}]`
}

export class PgVectorStore implements VectorStore {
  // Caches the distance metric per (bucket, index) so queryVectors doesn't
  // have to do a pg_index lookup on every call. Primed at createVectorIndex
  // time and falls back to lookupMetric on miss. Bounded + TTL-evicted so
  // it doesn't grow unbounded and self-heals from out-of-band drops.
  private readonly metricCache = new BaseTtlCache<string, DistanceMetric>({
    ttl: METRIC_CACHE_TTL_MS,
    max: METRIC_CACHE_MAX,
    updateAgeOnGet: true,
  })

  constructor(private readonly knex: KnexResolver) {}

  private db(): Knex {
    return resolveKnex(this.knex)
  }

  private cacheKey(bucket: string, index: string): string {
    return `${bucket}\x00${index}`
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
    // pgvector's HNSW operator classes (vector_cosine_ops, vector_l2_ops) cap
    // out at 2000 dimensions for FP32 vectors. Setting this stricter than the
    // S3Vectors-side validation (4096) so we fail loudly at create time rather
    // than at INDEX-build time with an opaque pgvector error.
    if (!dimension || !Number.isInteger(dimension) || dimension < 1 || dimension > 2_000) {
      throw ERRORS.InvalidParameter('dimension', {
        message: `Invalid dimension for pgvector HNSW: ${dimension} (must be 1..2000)`,
      })
    }

    const choice = distanceChoice(command.distanceMetric)
    const table = tableName(command.vectorBucketName, command.indexName)

    return handlePgVectorError(
      async () => {
        const db = this.db()
        // Postgres doesn't allow parameter binding inside type modifiers like
        // `vector(N)` — N must be a literal at parse time. We've validated
        // `dimension` is an integer in [1, 2_000] above, so inlining is safe.
        await db.raw(
          `CREATE TABLE ${SCHEMA}.??
           (
             key text PRIMARY KEY,
             embedding vector(${dimension}) NOT NULL,
             metadata jsonb NOT NULL DEFAULT '{}'::jsonb
           )`,
          [table]
        )
        await db.raw(`CREATE INDEX ?? ON ${SCHEMA}.?? USING hnsw (embedding ${choice.opClass})`, [
          `${table}_hnsw`,
          table,
        ])
        // Prime the metric cache so subsequent queryVectors don't need a
        // round-trip lookup to pick the right distance operator. The two
        // names are validated non-empty at the top of this method.
        this.metricCache.set(
          this.cacheKey(command.vectorBucketName as string, command.indexName as string),
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
    return handlePgVectorError(
      async () => {
        await this.db().raw(`DROP TABLE IF EXISTS ${SCHEMA}.??`, [tableName(bucket, index)])
        this.metricCache.delete(this.cacheKey(bucket, index))
        return { $metadata: {} } as DeleteIndexCommandOutput
      },
      { type: 'vector-index', name: index }
    )
  }

  async putVectors(command: PutVectorsInput): Promise<PutVectorsOutput> {
    const bucket = command.vectorBucketName!
    const index = command.indexName!
    const vectors = command.vectors ?? []
    if (vectors.length === 0) return {} as PutVectorsOutput

    return handlePgVectorError(
      async () => {
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
            metadata: v.metadata ?? {},
          }
        })

        await this.db().raw(
          `INSERT INTO ?? (key, embedding, metadata)
           SELECT key, embedding::vector, metadata
           FROM jsonb_to_recordset(?::jsonb)
             AS x(key text, embedding text, metadata jsonb)
           ON CONFLICT (key) DO UPDATE
             SET embedding = EXCLUDED.embedding,
                 metadata  = EXCLUDED.metadata`,
          [qualifiedTable(bucket, index), JSON.stringify(rows)]
        )
        return {} as PutVectorsOutput
      },
      { type: 'vectors', name: index }
    )
  }

  async getVectors(input: GetVectorsCommandInput): Promise<GetVectorsCommandOutput> {
    const bucket = input.vectorBucketName!
    const index = input.indexName!
    const keys = input.keys ?? []
    if (keys.length === 0) return { vectors: [] } as unknown as GetVectorsCommandOutput

    const wantData = input.returnData === true
    const wantMeta = input.returnMetadata === true

    return handlePgVectorError(
      async () => {
        const cols = ['key']
        if (wantData) cols.push('embedding::text AS embedding')
        if (wantMeta) cols.push('metadata')
        const sql = `SELECT ${cols.join(', ')} FROM ?? WHERE key = ANY(?)`
        const result = await this.db().raw(sql, [qualifiedTable(bucket, index), keys])
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
    const keys = input.keys ?? []
    if (keys.length === 0) return {} as DeleteVectorsOutput

    return handlePgVectorError(
      async () => {
        await this.db().raw(`DELETE FROM ?? WHERE key = ANY(?)`, [
          qualifiedTable(bucket, index),
          keys,
        ])
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
    const wantDistance = input.returnDistance !== false
    const topK = input.topK ?? 10

    return handlePgVectorError(
      async () => {
        // The operator chosen for the distance expression AND for ORDER BY must
        // match the HNSW index's operator class — otherwise the index isn't
        // used and the returned distance is in the wrong metric. Resolve the
        // metric (cached after createVectorIndex) before assembling the query.
        const metric = await this.getOrLookupMetric(bucket, index)
        const distanceOp: '<=>' | '<->' = metric === 'euclidean' ? '<->' : '<=>'

        const cols: string[] = ['key']
        if (wantDistance) cols.push(`embedding ${distanceOp} ?::vector AS distance`)
        if (wantMeta) cols.push('metadata')

        const params: unknown[] = []
        if (wantDistance) params.push(toVectorLiteral(queryVector.float32 as number[]))

        let whereClause = ''
        if (input.filter) {
          const translated = translateFilter(input.filter as unknown as S3VectorFilter)
          // The translator emits $N placeholders in left-to-right document
          // order matching translated.params. Knex.raw uses `?` positionally,
          // so we strip the numbering and let knex consume params in order.
          whereClause = ' WHERE ' + translated.sql.replace(/\$\d+/g, '?')
          params.push(...translated.params)
        }

        params.push(toVectorLiteral(queryVector.float32 as number[]))
        params.push(topK)

        // Inline the table name — it's a fixed prefix + hex hash (safe).
        // Avoids mixing `??` with `?::vector` casts, which trips up knex's
        // placeholder counter.
        const sql = `SELECT ${cols.join(', ')} FROM ${qualifiedTable(bucket, index)}${whereClause}
                     ORDER BY embedding ${distanceOp} ?::vector ASC
                     LIMIT ?`
        const result = await this.db().raw(sql, params)
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
    const key = this.cacheKey(bucket, index)
    const cached = this.metricCache.get(key)
    if (cached) return cached
    const metric = await this.lookupMetric(bucket, index)
    this.metricCache.set(key, metric)
    return metric
  }

  async listVectors(input: ListVectorsInput): Promise<ListVectorsOutput> {
    const bucket = input.vectorBucketName!
    const index = input.indexName!
    const wantData = input.returnData === true
    const wantMeta = input.returnMetadata === true
    const maxResults = input.maxResults ?? 100
    const cursor = input.nextToken

    return handlePgVectorError(
      async () => {
        const cols = ['key']
        if (wantData) cols.push('embedding::text AS embedding')
        if (wantMeta) cols.push('metadata')

        const params: unknown[] = [qualifiedTable(bucket, index)]
        let whereClause = ''
        if (cursor) {
          whereClause = ' WHERE key > ?'
          params.push(cursor)
        }
        params.push(maxResults)

        const sql = `SELECT ${cols.join(', ')} FROM ??${whereClause}
                     ORDER BY key ASC LIMIT ?`
        const result = await this.db().raw(sql, params)
        const rows = result.rows as Array<{
          key: string
          embedding?: string
          metadata?: DocumentType
        }>
        const nextToken = rows.length === maxResults ? rows[rows.length - 1].key : undefined
        return {
          vectors: rows.map((r) => ({
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
      const result = await this.db().raw(
        `SELECT am.amname, opc.opcname
         FROM pg_index i
         JOIN pg_class ic   ON ic.oid = i.indexrelid
         JOIN pg_class tc   ON tc.oid = i.indrelid
         JOIN pg_namespace n ON n.oid = tc.relnamespace
         JOIN pg_am am      ON am.oid = ic.relam
         JOIN pg_opclass opc ON opc.oid = ANY(i.indclass)
         WHERE n.nspname = ? AND tc.relname = ? AND am.amname = 'hnsw'
         LIMIT 1`,
        [SCHEMA, table]
      )
      const op = result.rows?.[0]?.opcname as string | undefined
      if (op === 'vector_l2_ops') return 'euclidean'
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
