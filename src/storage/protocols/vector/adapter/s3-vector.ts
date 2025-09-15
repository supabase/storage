import {
  CreateIndexCommand,
  CreateIndexCommandInput,
  CreateIndexCommandOutput,
  DeleteIndexCommand,
  DeleteIndexCommandInput,
  DeleteIndexCommandOutput,
  DeleteVectorsCommand,
  DeleteVectorsInput,
  DeleteVectorsOutput,
  GetVectorsCommand,
  GetVectorsCommandInput,
  GetVectorsCommandOutput,
  ListVectorsCommand,
  ListVectorsInput,
  ListVectorsOutput,
  PutVectorsCommand,
  PutVectorsInput,
  PutVectorsOutput,
  QueryVectorsCommand,
  QueryVectorsInput,
  QueryVectorsOutput,
  S3VectorsClient,
} from '@aws-sdk/client-s3vectors'
import { getConfig } from '../../../../config'

export interface VectorStore {
  createVectorIndex(command: CreateIndexCommandInput): Promise<CreateIndexCommandOutput>
  deleteVectorIndex(param: DeleteIndexCommandInput): Promise<DeleteIndexCommandOutput>
  putVectors(command: PutVectorsInput): Promise<PutVectorsOutput>
  listVectors(command: ListVectorsInput): Promise<ListVectorsOutput>

  queryVectors(queryInput: QueryVectorsInput): Promise<QueryVectorsOutput>

  deleteVectors(deleteVectorsInput: DeleteVectorsInput): Promise<DeleteVectorsOutput>

  getVectors(getVectorsInput: GetVectorsCommandInput): Promise<GetVectorsCommandOutput>
}

const { storageS3Region, vectorBucketRegion } = getConfig()

export function createS3VectorClient() {
  const s3VectorClient = new S3VectorsClient({
    region: vectorBucketRegion || storageS3Region,
  })

  return new S3VectorsClient(s3VectorClient)
}

export class S3Vector implements VectorStore {
  constructor(protected readonly s3VectorClient: S3VectorsClient) {}

  getVectors(getVectorsInput: GetVectorsCommandInput): Promise<GetVectorsCommandOutput> {
    return this.s3VectorClient.send(new GetVectorsCommand(getVectorsInput))
  }

  deleteVectors(deleteVectorsInput: DeleteVectorsInput): Promise<DeleteVectorsOutput> {
    return this.s3VectorClient.send(new DeleteVectorsCommand(deleteVectorsInput))
  }

  queryVectors(queryInput: QueryVectorsInput): Promise<QueryVectorsOutput> {
    return this.s3VectorClient.send(new QueryVectorsCommand(queryInput))
  }

  async listVectors(command: ListVectorsInput): Promise<ListVectorsOutput> {
    return this.s3VectorClient.send(new ListVectorsCommand(command))
  }

  putVectors(command: PutVectorsInput): Promise<PutVectorsOutput> {
    const input = new PutVectorsCommand(command)

    return this.s3VectorClient.send(input)
  }

  deleteVectorIndex(param: DeleteIndexCommandInput): Promise<DeleteIndexCommandOutput> {
    const command = new DeleteIndexCommand(param)

    return this.s3VectorClient.send(command)
  }

  createVectorIndex(command: CreateIndexCommandInput): Promise<CreateIndexCommandOutput> {
    const createIndexCommand = new CreateIndexCommand(command)

    return this.s3VectorClient.send(createIndexCommand)
  }
}
