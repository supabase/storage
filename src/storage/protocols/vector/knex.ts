import { Knex } from 'knex'
import { VectorIndex } from '@storage/schemas/vector'
import { ERRORS } from '@internal/errors'

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

export interface VectorMetadataDB {
  createVectorIndex(data: CreateVectorIndexParams): Promise<DBVectorIndex>
  withTransaction<T>(fn: (db: KnexVectorMetadataDB) => T): Promise<T>
  deleteVectorIndex(vectorIndexName: string): Promise<void>
  getIndex(bucketId: string, name: string): Promise<DBVectorIndex>

  listIndexes(command: ListIndexesInput): Promise<ListIndexesResult>
}

export class KnexVectorMetadataDB implements VectorMetadataDB {
  constructor(protected readonly knex: Knex) {}

  async listIndexes(command: ListIndexesInput): Promise<ListIndexesResult> {
    const maxResults = command.maxResults ? Math.min(command.maxResults, 500) : 500

    const query = this.knex
      .select<DBVectorIndex[]>('id', 'bucket_id', 'created_at')
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
      .select('*')
      .table('vector_indexes')
      .where({ vectorBucketName: bucketId, indexName: name })
      .first<DBVectorIndex>()

    if (!index) {
      throw ERRORS.NoSuchVectorIndex(name)
    }
    return index
  }

  createVectorIndex(data: CreateVectorIndexParams) {
    return this.knex.table('vector_indexes').insert<DBVectorIndex>(data)
  }

  withTransaction<T>(fn: (db: KnexVectorMetadataDB) => T): Promise<T> {
    return this.knex.transaction(async (trx) => {
      const trxDb = new KnexVectorMetadataDB(trx)
      return fn(trxDb)
    })
  }

  deleteVectorIndex(vectorIndexName: string): Promise<void> {
    return this.knex.table('vector_indexes').where({ indexName: vectorIndexName }).del()
  }
}
