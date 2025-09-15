import {
  CreateIndexInput,
  DeleteIndexInput,
  DistanceMetric,
  GetIndexCommandInput,
  ListIndexesInput,
  MetadataConfiguration,
} from '@aws-sdk/client-s3vectors'
import { VectorMetadataDB } from './knex'
import { VectorStore } from './adapter/s3-vector'
import { ERRORS } from '@internal/errors'
import { GetIndexOutput } from '@aws-sdk/client-s3vectors/dist-types/models/models_0'

export class VectorStoreManager {
  constructor(
    protected readonly vectorStore: VectorStore,
    protected readonly db: VectorMetadataDB,
    protected readonly config: { tenantId: string }
  ) {}

  protected getIndexName(name: string) {
    return `${this.config.tenantId}-${name}`
  }

  async createVectorIndex(command: CreateIndexInput): Promise<void> {
    if (!command.indexName) {
      throw ERRORS.MissingParameter('indexName')
    }

    const createIndexInput = {
      ...command,
      indexName: this.getIndexName(command.indexName),
    }

    await this.db.createVectorIndex({
      dataType: createIndexInput.dataType!,
      dimension: createIndexInput.dimension!,
      distanceMetric: createIndexInput.distanceMetric!,
      indexName: createIndexInput.indexName,
      metadataConfiguration: createIndexInput.metadataConfiguration,
      vectorBucketName: createIndexInput.vectorBucketName!,
    })

    await this.vectorStore.createVectorIndex(createIndexInput)
  }

  async deleteIndex(command: DeleteIndexInput): Promise<void> {
    if (!command.indexName) {
      throw ERRORS.MissingParameter('indexName')
    }

    const vectorIndexName = this.getIndexName(command.indexName)
    await this.db.deleteVectorIndex(vectorIndexName)
    await this.vectorStore.deleteVectorIndex({ indexName: vectorIndexName })
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

  listIndexes(command: ListIndexesInput) {
    if (!command.vectorBucketName) {
      throw ERRORS.MissingParameter('vectorBucketName')
    }

    return this.db.listIndexes({
      bucketId: command.vectorBucketName,
      maxResults: command.maxResults,
      nextToken: command.nextToken,
      prefix: command.prefix,
    })
  }
}
