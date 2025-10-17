import {
  CreateIndexInput,
  DeleteIndexInput,
  DistanceMetric,
  GetIndexCommandInput,
  ListIndexesInput,
  MetadataConfiguration,
  GetIndexOutput,
  PutVectorsInput,
  ListVectorsInput,
  ListVectorBucketsInput,
  QueryVectorsInput,
  DeleteVectorsInput,
  GetVectorBucketInput,
  GetVectorsCommandInput,
  ConflictException,
} from '@aws-sdk/client-s3vectors'
import { VectorMetadataDB } from './knex'
import { VectorStore } from './adapter/s3-vector'
import { ERRORS } from '@internal/errors'

interface VectorStoreConfig {
  tenantId: string
  vectorBucketName: string
  maxBucketCount: number
  maxIndexCount: number
}

export class VectorStoreManager {
  constructor(
    protected readonly vectorStore: VectorStore,
    protected readonly db: VectorMetadataDB,
    protected readonly config: VectorStoreConfig
  ) {}

  protected getIndexName(name: string) {
    return `${this.config.tenantId}-${name}`
  }

  async createBucket(bucketName: string): Promise<void> {
    await this.db.withTransaction(
      async (tnx) => {
        const bucketCount = await tnx.countBuckets()
        if (bucketCount >= this.config.maxBucketCount) {
          throw ERRORS.S3VectorMaxBucketsExceeded(this.config.maxBucketCount)
        }

        try {
          await tnx.createVectorBucket(bucketName)
        } catch (e) {
          if (e instanceof ConflictException) {
            return
          }
          throw e
        }
      },
      { isolationLevel: 'serializable' }
    )
  }

  async deleteBucket(bucketName: string): Promise<void> {
    await this.db.withTransaction(
      async (tx) => {
        const indexes = await tx.listIndexes({ bucketId: bucketName, maxResults: 1 })

        if (indexes.indexes.length > 0) {
          throw ERRORS.S3VectorBucketNotEmpty(bucketName)
        }

        await tx.deleteVectorBucket(bucketName)
      },
      { isolationLevel: 'serializable' }
    )
  }

  async getBucket(command: GetVectorBucketInput) {
    if (!command.vectorBucketName) {
      throw ERRORS.MissingParameter('vectorBucketName')
    }

    const vectorBucket = await this.db.findVectorBucket(command.vectorBucketName)

    return {
      vectorBucket: {
        vectorBucketName: vectorBucket.id,
        creationTime: vectorBucket.created_at
          ? Math.floor(vectorBucket.created_at.getTime() / 1000)
          : undefined,
      },
    }
  }

  async listBuckets(command: ListVectorBucketsInput) {
    const bucketResult = await this.db.listBuckets({
      maxResults: command.maxResults,
      nextToken: command.nextToken,
      prefix: command.prefix,
    })

    return {
      vectorBuckets: bucketResult.vectorBuckets.map((bucket) => ({
        vectorBucketName: bucket.id,
        creationTime: bucket.created_at
          ? Math.floor(bucket.created_at.getTime() / 1000)
          : undefined,
      })),
      nextToken: bucketResult.nextToken,
    }
  }

  // Store it in MultiTenantDB
  // Queue Job in the same transaction
  // Poll for job completion
  async createVectorIndex(command: CreateIndexInput): Promise<void> {
    if (!command.indexName) {
      throw ERRORS.MissingParameter('indexName')
    }

    if (!command.vectorBucketName) {
      throw ERRORS.MissingParameter('vectorBucketName')
    }

    await this.db.findVectorBucket(command.vectorBucketName)

    const createIndexInput = {
      ...command,
      indexName: this.getIndexName(command.indexName),
    }

    await this.db.withTransaction(
      async (tx) => {
        const indexCount = await tx.countIndexes(command.vectorBucketName!)

        if (indexCount >= this.config.maxIndexCount) {
          throw ERRORS.S3VectorMaxIndexesExceeded(this.config.maxIndexCount)
        }

        await tx.createVectorIndex({
          dataType: createIndexInput.dataType!,
          dimension: createIndexInput.dimension!,
          distanceMetric: createIndexInput.distanceMetric!,
          indexName: command.indexName!,
          metadataConfiguration: createIndexInput.metadataConfiguration,
          vectorBucketName: command.vectorBucketName!,
        })

        try {
          await this.vectorStore.createVectorIndex({
            ...createIndexInput,
            vectorBucketName: this.config.vectorBucketName,
          })
        } catch (e) {
          if (e instanceof ConflictException) {
            return
          }

          throw e
        }
      },
      { isolationLevel: 'serializable' }
    )
  }

  async deleteIndex(command: DeleteIndexInput): Promise<void> {
    if (!command.indexName) {
      throw ERRORS.MissingParameter('indexName')
    }

    if (!command.vectorBucketName) {
      throw ERRORS.MissingParameter('vectorBucketName')
    }

    await this.db.findVectorIndexForBucket(command.vectorBucketName, command.indexName)

    const vectorIndexName = this.getIndexName(command.indexName)

    await this.db.withTransaction(async (tx) => {
      await tx.deleteVectorIndex(command.vectorBucketName!, command.indexName!)

      await this.vectorStore.deleteVectorIndex({
        vectorBucketName: this.config.vectorBucketName,
        indexName: vectorIndexName,
      })
    })
  }

  async getIndex(command: GetIndexCommandInput): Promise<GetIndexOutput> {
    if (!command.indexName) {
      throw ERRORS.MissingParameter('indexName')
    }

    if (!command.vectorBucketName) {
      throw ERRORS.MissingParameter('vectorBucketName')
    }

    const index = await this.db.getIndex(command.vectorBucketName, command.indexName)

    return {
      index: {
        indexName: index.name,
        dataType: index.data_type as 'float32',
        dimension: index.dimension,
        distanceMetric: index.distance_metric as DistanceMetric,
        metadataConfiguration: index.metadata_configuration as MetadataConfiguration,
        vectorBucketName: index.bucket_id,
        creationTime: index.created_at,
        indexArn: undefined,
      },
    }
  }

  async listIndexes(command: ListIndexesInput) {
    if (!command.vectorBucketName) {
      throw ERRORS.MissingParameter('vectorBucketName')
    }

    const result = await this.db.listIndexes({
      bucketId: command.vectorBucketName,
      maxResults: command.maxResults,
      nextToken: command.nextToken,
      prefix: command.prefix,
    })

    return {
      indexes: result.indexes.map((i) => ({
        indexName: i.name,
        vectorBucketName: i.bucket_id,
        creationTime: Math.floor(i.created_at.getTime() / 1000),
      })),
    }
  }

  async putVectors(command: PutVectorsInput) {
    if (!command.indexName) {
      throw ERRORS.MissingParameter('indexName')
    }

    if (!command.vectorBucketName) {
      throw ERRORS.MissingParameter('vectorBucketName')
    }

    await this.db.findVectorIndexForBucket(command.vectorBucketName, command.indexName)

    const putVectorsInput = {
      ...command,
      vectorBucketName: this.config.vectorBucketName,
      indexName: this.getIndexName(command.indexName),
    }
    await this.vectorStore.putVectors(putVectorsInput)
  }

  async deleteVectors(command: DeleteVectorsInput) {
    if (!command.indexName) {
      throw ERRORS.MissingParameter('indexName')
    }

    if (!command.vectorBucketName) {
      throw ERRORS.MissingParameter('vectorBucketName')
    }

    await this.db.findVectorIndexForBucket(command.vectorBucketName, command.indexName)

    const deleteVectorsInput = {
      ...command,
      vectorBucketName: this.config.vectorBucketName,
      indexName: this.getIndexName(command.indexName),
    }

    return this.vectorStore.deleteVectors(deleteVectorsInput)
  }

  async listVectors(command: ListVectorsInput) {
    if (!command.indexName) {
      throw ERRORS.MissingParameter('indexName')
    }

    if (!command.vectorBucketName) {
      throw ERRORS.MissingParameter('vectorBucketName')
    }

    await this.db.findVectorIndexForBucket(command.vectorBucketName, command.indexName)

    const listVectorsInput = {
      ...command,
      vectorBucketName: this.config.vectorBucketName,
      indexName: this.getIndexName(command.indexName),
    }

    const result = await this.vectorStore.listVectors(listVectorsInput)

    return {
      vectors: result.vectors,
      nextToken: result.nextToken,
    }
  }

  async queryVectors(command: QueryVectorsInput) {
    if (!command.indexName) {
      throw ERRORS.MissingParameter('indexName')
    }

    if (!command.vectorBucketName) {
      throw ERRORS.MissingParameter('vectorBucketName')
    }

    await this.db.findVectorIndexForBucket(command.vectorBucketName, command.indexName)

    const queryInput = {
      ...command,
      vectorBucketName: this.config.vectorBucketName,
      indexName: this.getIndexName(command.indexName),
    }
    return this.vectorStore.queryVectors(queryInput)
  }

  async getVectors(command: GetVectorsCommandInput) {
    if (!command.indexName) {
      throw ERRORS.MissingParameter('indexName')
    }

    if (!command.vectorBucketName) {
      throw ERRORS.MissingParameter('vectorBucketName')
    }

    await this.db.findVectorIndexForBucket(command.vectorBucketName, command.indexName)

    const getVectorsInput = {
      ...command,
      vectorBucketName: this.config.vectorBucketName,
      indexName: this.getIndexName(command.indexName),
    }

    const result = await this.vectorStore.getVectors(getVectorsInput)

    return {
      vectors: result.vectors,
    }
  }
}
