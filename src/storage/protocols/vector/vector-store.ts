import { CreateIndexCommandInput, CreateIndexCommandOutput } from '@aws-sdk/client-s3vectors'
import { VectorDB } from './knex'
import { VectorStore } from './s3-vector'

export class VectorStoreManager {
  constructor(
    protected readonly vectorStore: VectorStore,
    protected readonly db: VectorDB,
    protected readonly config: { tenantId: string }
  ) {}

  createVectorIndex(command: CreateIndexCommandInput): Promise<CreateIndexCommandOutput> {
    const vectorIndexName = `${this.config.tenantId}-${command.indexName}`
    return this.vectorStore.createVectorIndex({
      ...command,
      indexName: vectorIndexName,
    })
  }
}
