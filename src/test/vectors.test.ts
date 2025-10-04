'use strict'

import app from '../app'
import { getConfig } from '../config'
import { FastifyInstance } from 'fastify'
import { useMockObject, useMockQueue } from './common'
import {
  CreateIndexCommandOutput,
  DeleteVectorsOutput,
  GetVectorsCommandOutput,
  ListVectorsOutput,
  PutVectorsOutput,
  QueryVectorsOutput,
} from '@aws-sdk/client-s3vectors'
import { KnexVectorMetadataDB, VectorStoreManager } from '@storage/protocols/vector'
import { useStorage } from './utils/storage'

const { serviceKeyAsync } = getConfig()

let appInstance: FastifyInstance
let serviceToken: string

// Use the common mock helpers
useMockObject()
useMockQueue()

jest.mock('@storage/protocols/vector/adapter/s3-vector')

const storageTest = useStorage()
const mockVectorStore = {
  deleteVectorIndex: jest.fn().mockResolvedValue({} as CreateIndexCommandOutput),
  createVectorIndex: jest.fn().mockResolvedValue({} as CreateIndexCommandOutput),
  putVectors: jest.fn().mockResolvedValue({} as PutVectorsOutput),
  listVectors: jest.fn().mockResolvedValue({} as ListVectorsOutput),
  queryVectors: jest.fn().mockResolvedValue({} as QueryVectorsOutput),
  deleteVectors: jest.fn().mockResolvedValue({} as DeleteVectorsOutput),
  getVectors: jest.fn().mockResolvedValue({} as GetVectorsCommandOutput),
}

beforeEach(async () => {
  jest.clearAllMocks()

  appInstance = app()

  // Create service role token
  serviceToken = await serviceKeyAsync

  // Create real S3Vector instance with mocked client and mock DB
  const mockVectorDB = new KnexVectorMetadataDB(storageTest.database.connection.pool.acquire())
  const s3Vector = new VectorStoreManager(mockVectorStore, mockVectorDB, {
    tenantId: 'test-tenant',
    vectorBucketName: 'test-bucket',
  })

  // Decorate fastify instance with real S3Vector
  appInstance.decorate('s3Vector', s3Vector)
})

afterEach(async () => {
  await appInstance.close()
  await storageTest.database.connection.dispose()
})

describe('POST /vectors/CreateIndex', () => {
  const validCreateIndexRequest = {
    dataType: 'float32',
    dimension: 1536,
    distanceMetric: 'cosine',
    indexName: 'test-index',
    vectorBucketName: 'test-bucket',
    metadataConfiguration: {
      nonFilterableMetadataKeys: ['key1', 'key2'],
    },
  }

  it('should create vector index successfully with valid request', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      headers: {
        authorization: `Bearer ${serviceToken}`,
      },
      payload: validCreateIndexRequest,
    })

    expect(response.statusCode).toBe(200)

    // Verify the CreateIndexCommand was called with correct parameters including tenantId prefix
    const createIndexCommand = mockVectorStore.createVectorIndex.mock.calls[0][0] as unknown as {
      input: Record<string, unknown>
    }
    expect(createIndexCommand.input.indexName).toBe('test-tenant-test-index')
    expect(createIndexCommand.input.dataType).toBe('float32')
    expect(createIndexCommand.input.dimension).toBe(1536)
  })

  it('should require authentication with service role', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      payload: validCreateIndexRequest,
    })

    expect(response.statusCode).toBe(401)
    // Vector service not called when validation fails
  })

  it('should reject request with invalid JWT role', async () => {
    const invalidToken = 'invalid-token'

    const response = await appInstance.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      headers: {
        authorization: `Bearer ${invalidToken}`,
      },
      payload: validCreateIndexRequest,
    })

    expect(response.statusCode).toBe(403)
    // Vector service not called when validation fails
  })

  it('should validate required fields', async () => {
    const incompleteRequest = {
      dataType: 'float32',
      dimension: 1536,
      // missing required fields
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      headers: {
        authorization: `Bearer ${serviceToken}`,
      },
      payload: incompleteRequest,
    })

    expect(response.statusCode).toBe(400)
    // Vector service not called when validation fails
  })

  it('should validate dataType enum', async () => {
    const invalidRequest = {
      ...validCreateIndexRequest,
      dataType: 'invalid-type',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      headers: {
        authorization: `Bearer ${serviceToken}`,
      },
      payload: invalidRequest,
    })

    expect(response.statusCode).toBe(400)
    // Vector service not called when validation fails
  })

  it('should validate distanceMetric enum', async () => {
    const invalidRequest = {
      ...validCreateIndexRequest,
      distanceMetric: 'invalid-metric',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      headers: {
        authorization: `Bearer ${serviceToken}`,
      },
      payload: invalidRequest,
    })

    expect(response.statusCode).toBe(400)
    // Vector service not called when validation fails
  })

  it('should validate dimension is a number', async () => {
    const invalidRequest = {
      ...validCreateIndexRequest,
      dimension: 'not-a-number',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      headers: {
        authorization: `Bearer ${serviceToken}`,
      },
      payload: invalidRequest,
    })

    expect(response.statusCode).toBe(400)
    // Vector service not called when validation fails
  })

  it('should validate metadataConfiguration structure', async () => {
    const invalidRequest = {
      ...validCreateIndexRequest,
      metadataConfiguration: {
        // missing required nonFilterableMetadataKeys
        invalidKey: 'value',
      },
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      headers: {
        authorization: `Bearer ${serviceToken}`,
      },
      payload: invalidRequest,
    })

    expect(response.statusCode).toBe(400)
    // Vector service not called when validation fails
  })

  it('should handle vector service not configured', async () => {
    // Mock app without s3Vector service
    const appWithoutVector = app()

    const response = await appWithoutVector.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      headers: {
        authorization: `Bearer ${serviceToken}`,
      },
      payload: validCreateIndexRequest,
    })

    expect(response.statusCode).toBe(400)
    const body = JSON.parse(response.body)
    expect(body.error).toBe('FeatureNotEnabled')
    expect(body.message).toBe('Vector service not configured')

    await appWithoutVector.close()
  })

  it('should handle S3Vector service errors', async () => {
    const s3Error = new Error('S3VectorsClient error')
    // Mock error - need to cast to bypass type restrictions
    mockVectorStore.createVectorIndex.mockRejectedValue(s3Error)

    const response = await appInstance.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      headers: {
        authorization: `Bearer ${serviceToken}`,
      },
      payload: validCreateIndexRequest,
    })

    expect(response.statusCode).toBe(500)
    expect(mockVectorStore.createVectorIndex).toHaveBeenCalledTimes(1)
  })

  it('should accept valid request without optional metadataConfiguration', async () => {
    const requestWithoutMetadata = {
      dataType: 'float32' as const,
      dimension: 1536,
      distanceMetric: 'euclidean' as const,
      indexName: 'test-index-2',
      vectorBucketName: 'test-bucket-2',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      headers: {
        authorization: `Bearer ${serviceToken}`,
      },
      payload: requestWithoutMetadata,
    })

    expect(response.statusCode).toBe(200)
    expect(mockVectorStore.createVectorIndex).toHaveBeenCalledTimes(1)

    // Verify the CreateIndexCommand was called with correct parameters
    const createIndexCommand = mockVectorStore.createVectorIndex.mock.calls[0][1]
    expect(createIndexCommand.input.indexName).toBe('test-tenant-test-index-2')
    expect(createIndexCommand.input.distanceMetric).toBe('euclidean')
  })

  it('should validate nonFilterableMetadataKeys as array of strings', async () => {
    const invalidRequest = {
      ...validCreateIndexRequest,
      metadataConfiguration: {
        nonFilterableMetadataKeys: ['valid', 123, 'another-valid'],
      },
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/vectors/CreateIndex',
      headers: {
        authorization: `Bearer ${serviceToken}`,
      },
      payload: invalidRequest,
    })

    expect(response.statusCode).toBe(400)
    // Vector service not called when validation fails
  })
})
