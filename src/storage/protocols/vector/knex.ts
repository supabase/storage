import { ListVectorBucketsInput } from '@aws-sdk/client-s3vectors'
import { wait } from '@internal/concurrency'
import { ERRORS } from '@internal/errors'
import { hashStringToInt } from '@internal/hashing'
import { VectorBucket } from '@storage/schemas'
import { VectorIndex } from '@storage/schemas/vector'
import { Knex } from 'knex'
import { DatabaseError } from 'pg'

type DBVectorIndex = VectorIndex & { id: string; created_at: Date; updated_at: Date }

interface CreateVectorIndexParams {
  dataType: string
  dimension: number
  distanceMetric: string
  indexName: string
  metadataConfiguration?: {
    nonFilterableMetadataKeys?: string[]
  }
  vectorBucketName: string
}

export interface ListIndexesInput {
  bucketId: string
  maxResults?: number
  nextToken?: string | undefined
  prefix?: string | undefined
}

export interface ListIndexesResult {
  indexes: Pick<DBVectorIndex, 'name' | 'created_at' | 'bucket_id'>[]
  nextToken?: string
}

export interface ListBucketResult {
  vectorBuckets: VectorBucket[]
  nextToken?: string
}

export interface VectorMetadataDB {
  withTransaction<T>(
    fn: (db: KnexVectorMetadataDB) => T,
    config?: Knex.TransactionConfig
  ): Promise<T>

  lockResource(resourceType: 'bucket' | 'index', resourceId: string): Promise<void>

  findVectorBucket(vectorBucketName: string): Promise<VectorBucket>
  createVectorBucket(bucketName: string): Promise<void>
  deleteVectorBucket(bucketName: string, vectorIndexName: string): Promise<void>
  listBuckets(param: ListVectorBucketsInput): Promise<ListBucketResult>
  countBuckets(): Promise<number>

  countIndexes(bucketId: string): Promise<number>
  createVectorIndex(data: CreateVectorIndexParams): Promise<DBVectorIndex>
  getIndex(bucketId: string, name: string): Promise<DBVectorIndex>
  listIndexes(command: ListIndexesInput): Promise<ListIndexesResult>
  deleteVectorIndex(bucketName: string, vectorIndexName: string): Promise<void>
  findVectorIndexForBucket(vectorBucketName: string, indexName: string): Promise<DBVectorIndex>
}

export class KnexVectorMetadataDB implements VectorMetadataDB {
  constructor(protected readonly knex: Knex) {}

  lockResource(resourceType: 'bucket' | 'index', resourceId: string): Promise<void> {
    const lockId = hashStringToInt(`vector:${resourceType}:${resourceId}`)
    return this.knex.raw('SELECT pg_advisory_xact_lock(?::bigint)', [lockId])
  }

  async countIndexes(bucketId: string): Promise<number> {
    const row = await this.knex
      .withSchema('storage')
      .table('vector_indexes')
      .where({ bucket_id: bucketId })
      .count<{ count: string }>('id as count')
      .first()
    return Number(row?.count ?? 0)
  }

  async countBuckets(): Promise<number> {
    const row = await this.knex
      .withSchema('storage')
      .table('buckets_vectors')
      .count<{ count: string }>('id as count')
      .first()

    return Number(row?.count ?? 0)
  }

  async listBuckets(param: ListVectorBucketsInput): Promise<ListBucketResult> {
    const query = this.knex.withSchema('storage').table<VectorBucket>('buckets_vectors')
    if (param.prefix) {
      query.where('id', 'like', `${param.prefix}%`)
    }

    if (param.nextToken) {
      query.andWhere('id', '>', param.nextToken)
    }
    const maxResults = param.maxResults ? Math.min(param.maxResults, 500) : 500

    const result = await query.orderBy('id', 'asc').limit(maxResults + 1)

    const hasMore = result.length > maxResults

    const buckets = result.slice(0, maxResults)

    return {
      vectorBuckets: buckets,
      nextToken: hasMore ? buckets[buckets.length - 1].id : undefined,
    }
  }

  async findVectorIndexForBucket(
    vectorBucketName: string,
    indexName: string
  ): Promise<DBVectorIndex> {
    const index = await this.knex
      .withSchema('storage')
      .select('*')
      .table<DBVectorIndex>('vector_indexes')
      .where({ bucket_id: vectorBucketName, name: indexName })
      .first()

    if (!index) {
      throw ERRORS.S3VectorNotFoundException('vector index', indexName)
    }
    return index
  }

  async findVectorBucket(vectorBucketName: string): Promise<VectorBucket> {
    const bucket = await this.knex
      .withSchema('storage')
      .table('buckets_vectors')
      .where({ id: vectorBucketName })
      .first()

    if (!bucket) {
      throw ERRORS.S3VectorNotFoundException('vector bucket', vectorBucketName)
    }

    return bucket
  }

  async createVectorBucket(bucketName: string): Promise<void> {
    try {
      await this.knex.withSchema('storage').table('buckets_vectors').insert({
        id: bucketName,
      })
    } catch (e) {
      if (e instanceof Error && e instanceof DatabaseError) {
        if (e.code === '23505') {
          throw ERRORS.S3VectorConflictException('vector bucket', bucketName)
        }
      }

      throw e
    }
  }

  async listIndexes(command: ListIndexesInput): Promise<ListIndexesResult> {
    const maxResults = command.maxResults ? Math.min(command.maxResults, 500) : 500

    const query = this.knex
      .withSchema('storage')
      .select<DBVectorIndex[]>('name', 'bucket_id', 'created_at')
      .from('vector_indexes')
      .where({ bucket_id: command.bucketId })
      .orderBy('name', 'asc')
      .table('vector_indexes')

    if (command.prefix) {
      query.andWhere('name', 'like', `${command.prefix}%`)
    }

    if (command.nextToken) {
      query.andWhere('id', '>', command.nextToken)
    }

    const result = await query.limit(maxResults + 1)
    const hasMore = result.length > maxResults

    const indexes = result.slice(0, maxResults)

    return {
      indexes,
      nextToken: hasMore ? indexes[indexes.length - 1].name : undefined,
    }
  }

  async getIndex(bucketId: string, name: string): Promise<DBVectorIndex> {
    const index = await this.knex
      .withSchema('storage')
      .select('*')
      .table('vector_indexes')
      .where({ bucket_id: bucketId, name })
      .first<DBVectorIndex>()

    if (!index) {
      throw ERRORS.S3VectorNotFoundException('vector index', name)
    }
    return index
  }

  async createVectorIndex(data: CreateVectorIndexParams) {
    try {
      return await this.knex
        .withSchema('storage')
        .table<DBVectorIndex>('vector_indexes')
        .insert<DBVectorIndex>({
          bucket_id: data.vectorBucketName,
          data_type: data.dataType,
          name: data.indexName,
          dimension: data.dimension,
          distance_metric: data.distanceMetric,
          metadata_configuration: data.metadataConfiguration,
        })
    } catch (e) {
      if (e instanceof Error && e instanceof DatabaseError) {
        if (e.code === '23505') {
          throw ERRORS.S3VectorConflictException('vector index', data.indexName)
        }
      }
      throw e
    }
  }

  async withTransaction<T>(
    fn: (db: KnexVectorMetadataDB) => T,
    config?: Knex.TransactionConfig
  ): Promise<T> {
    const maxRetries = 3
    let attempt = 0
    let lastError: Error | undefined = undefined

    while (attempt < maxRetries) {
      try {
        return await this.knex.transaction(async (trx) => {
          const trxDb = new KnexVectorMetadataDB(trx)
          const result = await fn(trxDb)
          return result
        }, config)
      } catch (error) {
        attempt++

        // Check if it's a serialization error (PostgreSQL error code 40001)
        if (error instanceof DatabaseError && error.code === '40001' && attempt < maxRetries) {
          // Exponential backoff: 20ms, 40ms, 80ms
          await wait(20 * Math.pow(2, attempt - 1))
          lastError = error
          continue
        }

        throw error
      }
    }

    throw ERRORS.TransactionError(`Transaction failed after maximum ${attempt} retries`, lastError)
  }

  deleteVectorIndex(bucketName: string, vectorIndexName: string): Promise<void> {
    return this.knex
      .withSchema('storage')
      .table('vector_indexes')
      .where({ bucket_id: bucketName, name: vectorIndexName })
      .del()
  }

  async deleteVectorBucket(bucketName: string) {
    await this.knex.withSchema('storage').table('buckets_vectors').where({ id: bucketName }).del()
  }
}
