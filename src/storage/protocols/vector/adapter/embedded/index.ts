import path from 'node:path'
import {
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
import { logger } from '@internal/monitoring'
import BaseTtlCache from '@isaacs/ttlcache'
import type { DocumentType } from '@smithy/types'
import { CreateVectorIndexInput, VectorStore } from '../s3-vector'
import { handleZVecError } from './error-handler'
import { S3VectorFilter, translateFilter } from './filter'

const VECTOR_FIELD = 'vector'
const METADATA_EXTRA_FIELD = '__metadata_extra'

type ZvecModule = typeof import('@zvec/zvec')
type ZVecCollection = ReturnType<ZvecModule['ZVecCreateAndOpen']>
type ZVecDataTypeValue = ZvecModule['ZVecDataType'][keyof ZvecModule['ZVecDataType']]
type ZVecMetricTypeValue = ZvecModule['ZVecMetricType'][keyof ZvecModule['ZVecMetricType']]

export interface EmbeddedVectorStoreOptions {
  basePath: string
  ttlMs?: number
}

export async function createEmbeddedVectorStore(
  opts: EmbeddedVectorStoreOptions
): Promise<EmbeddedVectorStore> {
  let zvec: ZvecModule
  try {
    zvec = (await import('@zvec/zvec')) as unknown as ZvecModule
  } catch (e) {
    throw ERRORS.S3VectorEmbeddedNotSupported(
      `@zvec/zvec native binding not installed: ${(e as Error).message}`
    )
  }
  return new EmbeddedVectorStore(zvec, opts)
}

function metricFromS3(metric: string | undefined, zvec: ZvecModule): ZVecMetricTypeValue {
  switch (metric) {
    case 'cosine':
      return zvec.ZVecMetricType.COSINE
    case 'euclidean':
      return zvec.ZVecMetricType.L2
    default:
      throw ERRORS.InvalidParameter('distanceMetric', {
        message: `Unsupported distance metric for embedded backend: ${metric}`,
      })
  }
}

function metricToS3(metric: ZVecMetricTypeValue | undefined, zvec: ZvecModule): DistanceMetric {
  if (metric === zvec.ZVecMetricType.COSINE) return 'cosine'
  if (metric === zvec.ZVecMetricType.L2) return 'euclidean'
  return 'cosine'
}

function dataTypeFromS3(dt: string | undefined, zvec: ZvecModule): ZVecDataTypeValue {
  switch (dt) {
    case 'float32':
      return zvec.ZVecDataType.VECTOR_FP32
    default:
      throw ERRORS.InvalidParameter('dataType', {
        message: `Unsupported data type for embedded backend: ${dt}`,
      })
  }
}

function scalarTypeFromDeclared(dataType: string, zvec: ZvecModule): ZVecDataTypeValue {
  switch (dataType) {
    case 'string':
      return zvec.ZVecDataType.STRING
    case 'number':
      return zvec.ZVecDataType.DOUBLE
    case 'boolean':
      return zvec.ZVecDataType.BOOL
    default:
      throw ERRORS.InvalidParameter('filterableMetadataKeys.dataType', {
        message: `Unsupported filterable metadata dataType: ${dataType}`,
      })
  }
}

function expectedJsTypeFor(declared: 'string' | 'number' | 'boolean'): string {
  return declared
}

function actualJsType(value: unknown): string {
  if (value === null) return 'null'
  return typeof value
}

interface CachedCollection {
  collection: ZVecCollection
  filterableKeys: Map<string, 'string' | 'number' | 'boolean'>
  distanceMetric: DistanceMetric
  destroyed?: boolean
}

export class EmbeddedVectorStore implements VectorStore {
  private readonly cache: BaseTtlCache<string, CachedCollection>
  private readonly basePath: string

  constructor(
    private readonly zvec: ZvecModule,
    opts: EmbeddedVectorStoreOptions
  ) {
    this.basePath = opts.basePath
    this.cache = new BaseTtlCache<string, CachedCollection>({
      ttl: opts.ttlMs ?? 60_000,
      updateAgeOnGet: true,
      dispose: (value) => {
        if (value.destroyed) return
        try {
          value.collection.closeSync()
        } catch (e) {
          logger.warn(
            { type: 'vector-embedded', error: (e as Error).message },
            '[EmbeddedVectorStore] failed to close evicted collection'
          )
        }
      },
    })
  }

  private cacheKey(bucket: string, index: string): string {
    return `${bucket}/${index}`
  }

  private collectionPath(bucket: string, index: string): string {
    return path.join(this.basePath, bucket, index)
  }

  private extractFilterableKeys(
    collection: ZVecCollection
  ): Map<string, 'string' | 'number' | 'boolean'> {
    const out = new Map<string, 'string' | 'number' | 'boolean'>()
    for (const f of collection.schema.fields()) {
      if (f.name === METADATA_EXTRA_FIELD) continue
      switch (f.dataType) {
        case this.zvec.ZVecDataType.STRING:
          out.set(f.name, 'string')
          break
        case this.zvec.ZVecDataType.DOUBLE:
        case this.zvec.ZVecDataType.FLOAT:
        case this.zvec.ZVecDataType.INT32:
        case this.zvec.ZVecDataType.INT64:
        case this.zvec.ZVecDataType.UINT32:
        case this.zvec.ZVecDataType.UINT64:
          out.set(f.name, 'number')
          break
        case this.zvec.ZVecDataType.BOOL:
          out.set(f.name, 'boolean')
          break
      }
    }
    return out
  }

  private extractDistanceMetric(collection: ZVecCollection): DistanceMetric {
    try {
      const v = collection.schema.vector(VECTOR_FIELD)
      const params = v.indexParams as { metricType?: ZVecMetricTypeValue } | undefined
      return metricToS3(params?.metricType, this.zvec)
    } catch {
      return 'cosine'
    }
  }

  private getOrOpen(bucket: string, index: string): CachedCollection {
    const key = this.cacheKey(bucket, index)
    const cached = this.cache.get(key)
    if (cached) return cached

    const collection = this.zvec.ZVecOpen(this.collectionPath(bucket, index))
    const entry: CachedCollection = {
      collection,
      filterableKeys: this.extractFilterableKeys(collection),
      distanceMetric: this.extractDistanceMetric(collection),
    }
    this.cache.set(key, entry)
    return entry
  }

  async createVectorIndex(command: CreateVectorIndexInput): Promise<CreateIndexCommandOutput> {
    if (!command.indexName || !command.vectorBucketName) {
      throw ERRORS.MissingParameter('indexName/vectorBucketName')
    }
    if (!command.filterableMetadataKeys) {
      throw ERRORS.S3VectorEmbeddedNotSupported(
        'createIndex requires filterableMetadataKeys when VECTOR_BACKEND=embedded'
      )
    }

    const bucket = command.vectorBucketName
    const index = command.indexName
    const dimension = command.dimension!
    const metricType = metricFromS3(command.distanceMetric, this.zvec)
    const vectorDataType = dataTypeFromS3(command.dataType, this.zvec)

    const filterableKeys = command.filterableMetadataKeys
    const seenNames = new Set<string>()
    for (const k of filterableKeys) {
      if (k.name === METADATA_EXTRA_FIELD || k.name === VECTOR_FIELD) {
        throw ERRORS.InvalidParameter('filterableMetadataKeys.name', {
          message: `Reserved name: ${k.name}`,
        })
      }
      if (seenNames.has(k.name)) {
        throw ERRORS.InvalidParameter('filterableMetadataKeys.name', {
          message: `Duplicate filterable key: ${k.name}`,
        })
      }
      seenNames.add(k.name)
    }

    const schema = new this.zvec.ZVecCollectionSchema({
      name: index,
      vectors: [
        {
          name: VECTOR_FIELD,
          dataType: vectorDataType,
          dimension,
          indexParams: {
            indexType: this.zvec.ZVecIndexType.HNSW,
            metricType,
          },
        },
      ],
      fields: [
        ...filterableKeys.map((k) => ({
          name: k.name,
          dataType: scalarTypeFromDeclared(k.dataType, this.zvec),
          nullable: true,
          indexParams: {
            indexType: this.zvec.ZVecIndexType.INVERT,
          },
        })),
        {
          name: METADATA_EXTRA_FIELD,
          dataType: this.zvec.ZVecDataType.STRING,
          nullable: true,
        },
      ],
    })

    return handleZVecError(
      () => {
        const collection = this.zvec.ZVecCreateAndOpen(this.collectionPath(bucket, index), schema)
        const filterableMap = new Map<string, 'string' | 'number' | 'boolean'>(
          filterableKeys.map((k) => [k.name, k.dataType])
        )
        this.cache.set(this.cacheKey(bucket, index), {
          collection,
          filterableKeys: filterableMap,
          distanceMetric: command.distanceMetric as DistanceMetric,
        })
        return { $metadata: {} } as CreateIndexCommandOutput
      },
      { type: 'vector-index', name: index }
    )
  }

  async deleteVectorIndex(param: DeleteIndexCommandInput): Promise<DeleteIndexCommandOutput> {
    const bucket = param.vectorBucketName!
    const index = param.indexName!
    const key = this.cacheKey(bucket, index)

    return handleZVecError(
      () => {
        let entry = this.cache.get(key)
        if (!entry) {
          try {
            const collection = this.zvec.ZVecOpen(this.collectionPath(bucket, index))
            entry = {
              collection,
              filterableKeys: new Map(),
              distanceMetric: 'cosine',
            }
          } catch (e) {
            const code = (e as { code?: string }).code
            if (code === 'ZVEC_NOT_FOUND') {
              return { $metadata: {} } as DeleteIndexCommandOutput
            }
            throw e
          }
        }
        entry.destroyed = true
        entry.collection.destroySync()
        this.cache.delete(key)
        return { $metadata: {} } as DeleteIndexCommandOutput
      },
      { type: 'vector-index', name: index }
    )
  }

  async putVectors(command: PutVectorsInput): Promise<PutVectorsOutput> {
    const bucket = command.vectorBucketName!
    const index = command.indexName!
    const vectors = command.vectors ?? []

    return handleZVecError(
      () => {
        const entry = this.getOrOpen(bucket, index)
        const docs = vectors.map((v) => this.toZVecDoc(v, entry.filterableKeys))
        if (docs.length === 0) {
          return {} as PutVectorsOutput
        }
        const results = entry.collection.upsertSync(docs)
        const failed = results.find((r) => !r.ok)
        if (failed) {
          throw Object.assign(new Error(failed.message), { code: failed.code })
        }
        return {} as PutVectorsOutput
      },
      { type: 'vectors', name: index }
    )
  }

  async getVectors(input: GetVectorsCommandInput): Promise<GetVectorsCommandOutput> {
    const bucket = input.vectorBucketName!
    const index = input.indexName!
    const keys = input.keys ?? []

    return handleZVecError(
      () => {
        const entry = this.getOrOpen(bucket, index)
        const fetched = entry.collection.fetchSync(keys)
        const wantData = input.returnData === true
        const wantMeta = input.returnMetadata === true

        const out: NonNullable<GetVectorsCommandOutput['vectors']> = []
        for (const k of keys) {
          const doc = fetched[k]
          if (!doc) continue
          out.push({
            key: doc.id,
            data: wantData ? { float32: this.docToFloatArray(doc) } : undefined,
            metadata: wantMeta ? this.docToMetadata(doc, entry.filterableKeys) : undefined,
          })
        }
        return { vectors: out } as GetVectorsCommandOutput
      },
      { type: 'vectors', name: index }
    )
  }

  async deleteVectors(input: DeleteVectorsInput): Promise<DeleteVectorsOutput> {
    const bucket = input.vectorBucketName!
    const index = input.indexName!
    const keys = input.keys ?? []
    if (keys.length === 0) return {} as DeleteVectorsOutput

    return handleZVecError(
      () => {
        const entry = this.getOrOpen(bucket, index)
        entry.collection.deleteSync(keys)
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

    return handleZVecError(
      () => {
        const entry = this.getOrOpen(bucket, index)
        const filterStr = input.filter
          ? translateFilter(input.filter as unknown as S3VectorFilter)
          : undefined
        const wantMeta = input.returnMetadata === true
        const queryParams: Parameters<typeof entry.collection.querySync>[0] = {
          fieldName: VECTOR_FIELD,
          vector: queryVector.float32 as number[],
          topk: input.topK ?? 10,
          includeVector: false,
        }
        if (filterStr) queryParams.filter = filterStr
        if (!wantMeta) queryParams.outputFields = []
        const results = entry.collection.querySync(queryParams)
        const wantDistance = input.returnDistance !== false

        return {
          vectors: results.map((doc) => ({
            key: doc.id,
            distance: wantDistance ? doc.score : undefined,
            metadata: wantMeta ? this.docToMetadata(doc, entry.filterableKeys) : undefined,
          })),
          distanceMetric: entry.distanceMetric,
        } as QueryVectorsOutput
      },
      { type: 'vector-index', name: index }
    )
  }

  async listVectors(_input: ListVectorsInput): Promise<ListVectorsOutput> {
    throw ERRORS.S3VectorEmbeddedNotSupported('listVectors')
  }

  /**
   * Closes all cached collections and cancels the cache TTL timer. Used by tests
   * and graceful shutdown so the native zvec handles release before process exit.
   */
  shutdown(): void {
    this.cache.clear()
    this.cache.cancelTimer()
  }

  private toZVecDoc(
    v: NonNullable<PutVectorsInput['vectors']>[number],
    filterableKeys: Map<string, 'string' | 'number' | 'boolean'>
  ): { id: string; vectors: Record<string, number[]>; fields: Record<string, unknown> } {
    if (!v.key) {
      throw ERRORS.MissingParameter('vector.key')
    }
    const data = v.data
    if (!data || !data.float32) {
      throw ERRORS.MissingParameter('vector.data.float32')
    }

    const fields: Record<string, unknown> = {}
    const extra: Record<string, unknown> = {}

    if (v.metadata && typeof v.metadata === 'object') {
      for (const [name, raw] of Object.entries(v.metadata as Record<string, unknown>)) {
        const expected = filterableKeys.get(name)
        if (expected) {
          const got = actualJsType(raw)
          if (got !== expectedJsTypeFor(expected)) {
            throw ERRORS.S3VectorEmbeddedSchemaMismatch(name, expected, got)
          }
          fields[name] = raw
        } else {
          extra[name] = raw
        }
      }
    }

    if (Object.keys(extra).length > 0) {
      fields[METADATA_EXTRA_FIELD] = JSON.stringify(extra)
    }

    return {
      id: v.key,
      vectors: { [VECTOR_FIELD]: data.float32 as number[] },
      fields,
    }
  }

  private docToFloatArray(doc: { vectors?: Record<string, unknown> }): number[] {
    const v = doc.vectors?.[VECTOR_FIELD]
    if (!v) return []
    if (Array.isArray(v)) return v as number[]
    if (v instanceof Float32Array) return Array.from(v)
    return []
  }

  private docToMetadata(
    doc: { fields?: Record<string, unknown> },
    filterableKeys: Map<string, 'string' | 'number' | 'boolean'>
  ): DocumentType {
    const out: Record<string, DocumentType> = {}
    const fields = doc.fields ?? {}
    for (const name of filterableKeys.keys()) {
      if (name in fields && fields[name] !== null && fields[name] !== undefined) {
        out[name] = fields[name] as DocumentType
      }
    }
    const extraRaw = fields[METADATA_EXTRA_FIELD]
    if (typeof extraRaw === 'string' && extraRaw.length > 0) {
      try {
        const parsed = JSON.parse(extraRaw) as Record<string, DocumentType>
        for (const [k, v] of Object.entries(parsed)) {
          if (!(k in out)) out[k] = v
        }
      } catch {
        // ignore corrupt JSON
      }
    }
    return out
  }
}
