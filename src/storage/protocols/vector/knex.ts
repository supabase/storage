import { Knex } from 'knex'
import { VectorIndex } from '@storage/schemas/vector'

interface CreateVectorIndexParams {
  dataType: string
  dimension: number
  distanceMetric: string
  indexName: string
  metadataConfiguration?: {
    nonFilterableMetadataKeys: string[]
  }
  vectorBucketArn: string
  vectorBucketName: string
}

export interface VectorDB {
  createVectorIndex(data: CreateVectorIndexParams): Promise<VectorIndex>
}

export class KnexVectorDB implements VectorDB {
  constructor(protected readonly knex: Knex) {}

  createVectorIndex(data: CreateVectorIndexParams) {
    return this.knex.table('vector_indexes').insert<VectorIndex>(data)
  }
}
