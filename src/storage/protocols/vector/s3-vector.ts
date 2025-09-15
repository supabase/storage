import {
  CreateIndexCommand,
  CreateIndexCommandInput,
  CreateIndexCommandOutput,
  S3VectorsClient,
} from '@aws-sdk/client-s3vectors'
import { getConfig } from '../../../config'

export interface VectorStore {
  createVectorIndex(command: CreateIndexCommandInput): Promise<CreateIndexCommandOutput>
}

const { storageS3Region } = getConfig()

export function createS3VectorClient() {
  const s3VectorClient = new S3VectorsClient({
    region: storageS3Region,
  })

  return new S3VectorsClient(s3VectorClient)
}

export class S3Vector implements VectorStore {
  constructor(protected readonly s3VectorClient: S3VectorsClient) {}

  createVectorIndex(command: CreateIndexCommandInput): Promise<CreateIndexCommandOutput> {
    const createIndexCommand = new CreateIndexCommand(command)
    return this.s3VectorClient.send(createIndexCommand)
  }
}
