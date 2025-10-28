'use strict'

import app from '../app'
import { getConfig, mergeConfig } from '../config'
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
import { KnexVectorMetadataDB, VectorStore, VectorStoreManager } from '@storage/protocols/vector'
import { useStorage } from './utils/storage'
import { signJWT } from '@internal/auth'
import { SingleShard } from '@internal/sharding'

const { serviceKeyAsync, vectorS3Buckets, tenantId, jwtSecret } = getConfig()

const vectorBucketS3 = vectorS3Buckets[0]

let appInstance: FastifyInstance
let serviceToken: string

// Use the common mock helpers
useMockObject()
useMockQueue()

jest.mock('@storage/protocols/vector/adapter/s3-vector', () => {
  const mockS3Vector = {
    deleteVectorIndex: jest.fn().mockResolvedValue({} as CreateIndexCommandOutput),
    createVectorIndex: jest.fn().mockResolvedValue({} as CreateIndexCommandOutput),
    putVectors: jest.fn().mockResolvedValue({} as PutVectorsOutput),
    listVectors: jest.fn().mockResolvedValue({} as ListVectorsOutput),
    queryVectors: jest.fn().mockResolvedValue({} as QueryVectorsOutput),
    deleteVectors: jest.fn().mockResolvedValue({} as DeleteVectorsOutput),
    getVectors: jest.fn().mockResolvedValue({} as GetVectorsCommandOutput),
    createS3VectorClient: jest.fn().mockReturnValue({}),
  }

  return {
    S3Vector: jest.fn().mockImplementation(() => mockS3Vector),
    ...mockS3Vector,
  }
})

const mockVectorStore = jest.mocked<VectorStore>(
  jest.requireMock('@storage/protocols/vector/adapter/s3-vector')
)

let vectorBucketName: string
let s3Vector: VectorStoreManager

describe('Vectors API', () => {
  const storageTest = useStorage()

  beforeAll(async () => {
    appInstance = app()

    // Create service role token
    serviceToken = await serviceKeyAsync

    // Create real S3Vector instance with mocked client and mock DB
    const shard = new SingleShard({
      shardKey: 'test-bucket',
      capacity: 1000,
    })
    const mockVectorDB = new KnexVectorMetadataDB(storageTest.database.connection.pool.acquire())
    s3Vector = new VectorStoreManager(mockVectorStore, mockVectorDB, shard, {
      tenantId: 'test-tenant',
      maxBucketCount: Infinity,
      maxIndexCount: Infinity,
    })

    // Decorate fastify instance with real S3Vector
    appInstance.decorate('s3Vector', s3Vector)
  })

  afterAll(async () => {
    await appInstance.close()
    await storageTest.database.connection.dispose()
  })

  beforeEach(async () => {
    jest.clearAllMocks()
    jest.resetAllMocks()

    getConfig({ reload: true })
    mergeConfig({ vectorMaxBucketsCount: Infinity, vectorMaxIndexesCount: Infinity })

    vectorBucketName = `test-bucket-${Date.now()}`
    await s3Vector.createBucket(vectorBucketName)
  })

  describe('POST /vector/CreateIndex', () => {
    let validCreateIndexRequest: {
      dataType: 'float32'
      dimension: number
      distanceMetric: 'cosine' | 'euclidean'
      indexName: string
      vectorBucketName: string
      metadataConfiguration?: {
        nonFilterableMetadataKeys: string[]
      }
    }
    beforeEach(async () => {
      validCreateIndexRequest = {
        dataType: 'float32',
        dimension: 1536,
        distanceMetric: 'cosine',
        indexName: 'test-index',
        vectorBucketName: vectorBucketName,
        metadataConfiguration: {
          nonFilterableMetadataKeys: ['key1', 'key2'],
        },
      }
    })

    it('should create vector index successfully with valid request', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: validCreateIndexRequest,
      })

      expect(response.statusCode).toBe(200)

      // Verify the CreateIndexCommand was called with correct parameters including tenantId prefix
      const createIndexCommand = mockVectorStore.createVectorIndex
      expect(createIndexCommand).toBeCalledWith({
        ...validCreateIndexRequest,
        vectorBucketName: vectorBucketS3,
        indexName: `${tenantId}-test-index`,
      })

      const indexMetadata = await storageTest.database.connection.pool
        .acquire()
        .table('storage.vector_indexes')
        .where({
          name: validCreateIndexRequest.indexName,
          bucket_id: validCreateIndexRequest.vectorBucketName,
        })
        .first()

      expect(indexMetadata).toBeDefined()
      expect(indexMetadata?.data_type).toBe(validCreateIndexRequest.dataType)
      expect(indexMetadata?.dimension).toBe(validCreateIndexRequest.dimension)
      expect(indexMetadata?.distance_metric).toBe(validCreateIndexRequest.distanceMetric)
      expect(indexMetadata?.metadata_configuration).toEqual(
        validCreateIndexRequest.metadataConfiguration
      )
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        payload: validCreateIndexRequest,
      })

      expect(response.statusCode).toBe(403)
      // Vector service not called when validation fails
    })

    it('should reject request with invalid JWT role', async () => {
      const invalidToken = 'invalid-token'

      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${invalidToken}`,
        },
        payload: validCreateIndexRequest,
      })

      expect(response.statusCode).toBe(400)
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
        url: '/vector/CreateIndex',
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
        url: '/vector/CreateIndex',
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
        url: '/vector/CreateIndex',
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
        url: '/vector/CreateIndex',
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
        url: '/vector/CreateIndex',
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
      mergeConfig({ vectorEnabled: false })

      const response = await appWithoutVector.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: validCreateIndexRequest,
      })

      expect(response.statusCode).toBe(404)
      const body = JSON.parse(response.body)
      expect(body.error).toBe('Not Found')

      await appWithoutVector.close()
    })

    it('should handle S3Vector service errors', async () => {
      const s3Error = new Error('S3VectorsClient error')
      // Mock error - need to cast to bypass type restrictions
      mockVectorStore.createVectorIndex.mockRejectedValue(s3Error)

      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
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
        vectorBucketName: vectorBucketName,
      }

      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: requestWithoutMetadata,
      })

      expect(response.statusCode).toBe(200)
      expect(mockVectorStore.createVectorIndex).toHaveBeenCalledTimes(1)
      expect(mockVectorStore.createVectorIndex).toHaveBeenCalledWith({
        ...requestWithoutMetadata,
        vectorBucketName: vectorBucketS3,
        indexName: `${tenantId}-test-index-2`,
      })
    })
  })

  describe('POST /vector/CreateVectorBucket', () => {
    beforeEach(async () => {})

    it('should create vector bucket successfully with valid request', async () => {
      const newBucketName = `test-bucket-${Date.now()}-new`
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateVectorBucket',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: newBucketName,
        },
      })

      expect(response.statusCode).toBe(200)

      // Verify bucket was created in database
      const bucketRecord = await storageTest.database.connection.pool
        .acquire()
        .table('storage.buckets_vectors')
        .where({ id: newBucketName })
        .first()

      expect(bucketRecord).toBeDefined()
      expect(bucketRecord?.id).toBe(newBucketName)
      expect(bucketRecord?.created_at).toBeDefined()
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateVectorBucket',
        payload: {
          vectorBucketName: 'test-bucket',
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should reject request with invalid JWT role', async () => {
      const token = await signJWT({ role: 'auth', sub: '1234' }, jwtSecret, '1h')

      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateVectorBucket',
        headers: {
          authorization: `Bearer ${token}`,
        },
        payload: {
          vectorBucketName: 'test-bucket',
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should validate required fields', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateVectorBucket',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {},
      })

      expect(response.statusCode).toBe(400)
    })

    it('should handle duplicate bucket creation gracefully', async () => {
      // First creation
      const newVectorBucketName = `test-bucket-${Date.now()}-dup`
      const response1 = await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateVectorBucket',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: newVectorBucketName,
        },
      })

      expect(response1.statusCode).toBe(200)

      // Second creation should return conflict
      const response2 = await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateVectorBucket',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: newVectorBucketName,
        },
      })

      expect(response2.statusCode).toBe(409)
    })
  })

  describe('POST /vector/DeleteVectorBucket', () => {
    beforeEach(async () => {})
    it('should delete empty vector bucket successfully', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteVectorBucket',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(200)

      // Verify bucket was deleted from database
      const bucketRecord = await storageTest.database.connection.pool
        .acquire()
        .table('storage.buckets_vectors')
        .where({ id: vectorBucketName })
        .first()

      expect(bucketRecord).toBeUndefined()
    })

    it('should fail when trying to delete bucket with indexes', async () => {
      // First create an index in the bucket
      await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          dataType: 'float32',
          dimension: 1536,
          distanceMetric: 'cosine',
          indexName: 'test-index',
          vectorBucketName: vectorBucketName,
        },
      })

      // Try to delete the bucket
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteVectorBucket',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(400)
      const body = JSON.parse(response.body)
      expect(body.error).toBe('VectorBucketNotEmpty')
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteVectorBucket',
        payload: {
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should validate required fields', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteVectorBucket',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {},
      })

      expect(response.statusCode).toBe(400)
    })

    it('should handle non-existent bucket', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteVectorBucket',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: 'non-existent-bucket',
        },
      })

      expect(response.statusCode).toBe(200)
    })
  })

  describe('POST /vector/ListVectorBuckets', () => {
    beforeEach(async () => {
      // Create multiple buckets for listing
      await s3Vector.createBucket(`test-bucket-a-${Date.now()}`)
      await s3Vector.createBucket(`test-bucket-b-${Date.now()}`)
      await s3Vector.createBucket(`test-bucket-c-${Date.now()}`)
    })

    it('should list all vector buckets', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectorBuckets',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {},
      })

      expect(response.statusCode).toBe(200)
      const body = JSON.parse(response.body)
      expect(body.vectorBuckets).toBeDefined()
      expect(Array.isArray(body.vectorBuckets)).toBe(true)
      expect(body.vectorBuckets.length).toBeGreaterThan(0)

      // Verify structure of bucket objects
      body.vectorBuckets.forEach((bucket: any) => {
        expect(bucket.vectorBucketName).toBeDefined()
        expect(bucket.creationTime).toBeDefined()
        expect(typeof bucket.creationTime).toBe('number')
      })
    })

    it('should support maxResults parameter', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectorBuckets',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          maxResults: 2,
        },
      })

      expect(response.statusCode).toBe(200)
      const body = JSON.parse(response.body)
      expect(body.vectorBuckets.length).toBeLessThanOrEqual(2)
      if (body.vectorBuckets.length === 2) {
        expect(body.nextToken).toBeDefined()
      }
    })

    it('should support pagination with nextToken', async () => {
      const response1 = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectorBuckets',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          maxResults: 1,
        },
      })

      const body1 = JSON.parse(response1.body)

      if (body1.nextToken) {
        const response2 = await appInstance.inject({
          method: 'POST',
          url: '/vector/ListVectorBuckets',
          headers: {
            authorization: `Bearer ${serviceToken}`,
          },
          payload: {
            maxResults: 1,
            nextToken: body1.nextToken,
          },
        })

        expect(response2.statusCode).toBe(200)
        const body2 = JSON.parse(response2.body)
        expect(body2.vectorBuckets).toBeDefined()

        // Ensure different buckets are returned
        if (body2.vectorBuckets.length > 0) {
          expect(body1.vectorBuckets[0].vectorBucketName).not.toBe(
            body2.vectorBuckets[0].vectorBucketName
          )
        }
      }
    })

    it('should support prefix filtering', async () => {
      const prefix = 'test-bucket-a'
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectorBuckets',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          prefix,
        },
      })

      expect(response.statusCode).toBe(200)
      const body = JSON.parse(response.body)
      body.vectorBuckets.forEach((bucket: any) => {
        expect(bucket.vectorBucketName).toMatch(new RegExp(`^${prefix}`))
      })
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectorBuckets',
        payload: {},
      })

      expect(response.statusCode).toBe(403)
    })
  })

  describe('POST /vector/GetVectorBucket', () => {
    it('should get vector bucket details successfully', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetVectorBucket',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(200)
      const body = JSON.parse(response.body)
      expect(body.vectorBucket).toBeDefined()
      expect(body.vectorBucket.vectorBucketName).toBe(vectorBucketName)
      expect(body.vectorBucket.creationTime).toBeDefined()
      expect(typeof body.vectorBucket.creationTime).toBe('number')
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetVectorBucket',
        payload: {
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should validate required fields', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetVectorBucket',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {},
      })

      expect(response.statusCode).toBe(400)
    })

    it('should handle non-existent bucket', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetVectorBucket',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: 'non-existent-bucket',
        },
      })

      expect(response.statusCode).toBe(404)
    })
  })

  describe('POST /vector/DeleteIndex', () => {
    let indexName: string

    beforeEach(async () => {
      vectorBucketName = `test-delete-index-${Date.now()}`
      await s3Vector.createBucket(vectorBucketName)

      indexName = `test-index-${Date.now()}`
      // Create an index first

      await s3Vector.createVectorIndex({
        dataType: 'float32',
        dimension: 1536,
        distanceMetric: 'cosine',
        indexName: indexName,
        vectorBucketName: vectorBucketName,
      })
    })

    it('should delete vector index successfully', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          indexName: indexName,
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(200)

      // Verify the index was deleted from database
      const indexRecord = await storageTest.database.connection.pool
        .acquire()
        .table('storage.vector_indexes')
        .where({
          name: indexName,
          bucket_id: vectorBucketName,
        })
        .first()

      expect(indexRecord).toBeUndefined()

      // Verify deleteVectorIndex was called with correct parameters
      expect(mockVectorStore.deleteVectorIndex).toHaveBeenCalledWith({
        vectorBucketName: vectorBucketS3,
        indexName: `${tenantId}-${indexName}`,
      })
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteIndex',
        payload: {
          indexName: indexName,
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should validate required fields', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          indexName: indexName,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should validate indexName pattern', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          indexName: 'INVALID_NAME',
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should handle non-existent index', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          indexName: 'non-existent-index',
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(404)
    })
  })

  describe('POST /vector/ListIndexes', () => {
    beforeEach(async () => {
      // Create multiple indexes for listing
      await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          dataType: 'float32',
          dimension: 1536,
          distanceMetric: 'cosine',
          indexName: `index-a-${Date.now()}`,
          vectorBucketName: vectorBucketName,
        },
      })

      await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          dataType: 'float32',
          dimension: 768,
          distanceMetric: 'euclidean',
          indexName: `index-b-${Date.now()}`,
          vectorBucketName: vectorBucketName,
        },
      })
    })

    it('should list all indexes in a bucket', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListIndexes',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(200)
      const body = JSON.parse(response.body)
      expect(body.indexes).toBeDefined()
      expect(Array.isArray(body.indexes)).toBe(true)
      expect(body.indexes.length).toBeGreaterThanOrEqual(2)

      // Verify structure of index objects
      body.indexes.forEach((index: any) => {
        expect(index.indexName).toBeDefined()
        expect(index.vectorBucketName).toBe(vectorBucketName)
        expect(index.creationTime).toBeDefined()
        expect(typeof index.creationTime).toBe('number')
      })
    })

    it('should support maxResults parameter', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListIndexes',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          maxResults: 1,
        },
      })

      expect(response.statusCode).toBe(200)
      const body = JSON.parse(response.body)
      expect(body.indexes.length).toBeLessThanOrEqual(1)
    })

    it('should support prefix filtering', async () => {
      const prefix = 'index-a'
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListIndexes',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          prefix,
        },
      })

      expect(response.statusCode).toBe(200)
      const body = JSON.parse(response.body)
      body.indexes.forEach((index: any) => {
        expect(index.indexName).toMatch(new RegExp(`^${prefix}`))
      })
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListIndexes',
        payload: {
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should validate required fields', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListIndexes',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {},
      })

      expect(response.statusCode).toBe(400)
    })
  })

  describe('POST /vector/GetIndex', () => {
    let indexName: string

    beforeEach(async () => {
      indexName = `test-index-${Date.now()}`
      await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          dataType: 'float32',
          dimension: 1536,
          distanceMetric: 'cosine',
          indexName: indexName,
          vectorBucketName: vectorBucketName,
          metadataConfiguration: {
            nonFilterableMetadataKeys: ['key1'],
          },
        },
      })
    })

    it('should get index details successfully', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          indexName: indexName,
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(200)
      const body = JSON.parse(response.body)
      expect(body.index).toBeDefined()
      expect(body.index.indexName).toBe(indexName)
      expect(body.index.vectorBucketName).toBe(vectorBucketName)
      expect(body.index.dataType).toBe('float32')
      expect(body.index.dimension).toBe(1536)
      expect(body.index.distanceMetric).toBe('cosine')
      expect(body.index.metadataConfiguration).toEqual({
        nonFilterableMetadataKeys: ['key1'],
      })
      expect(body.index.creationTime).toBeDefined()
      expect(typeof body.index.creationTime).toBe('number')
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetIndex',
        payload: {
          indexName: indexName,
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should validate required fields', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          indexName: indexName,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should validate indexName pattern', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          indexName: 'INVALID_NAME',
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should handle non-existent index', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          indexName: 'non-existent-index',
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(404)
    })
  })

  describe('POST /vector/PutVectors', () => {
    let indexName: string

    beforeEach(async () => {
      indexName = `test-index-${Date.now()}`
      await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          dataType: 'float32',
          dimension: 3,
          distanceMetric: 'cosine',
          indexName: indexName,
          vectorBucketName: vectorBucketName,
        },
      })

      mockVectorStore.putVectors.mockResolvedValue({
        vectorKeys: [{ key: 'vec1' }, { key: 'vec2' }],
      } as PutVectorsOutput)
    })

    it('should put vector successfully', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/PutVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          vectors: [
            {
              key: 'vec1',
              data: {
                float32: [1.0, 2.0, 3.0],
              },
              metadata: {
                category: 'test',
              },
            },
            {
              data: {
                float32: [4.0, 5.0, 6.0],
              },
            },
          ],
        },
      })

      expect(response.statusCode).toBe(200)

      // Verify putVectors was called with correct parameters
      expect(mockVectorStore.putVectors).toHaveBeenCalledWith({
        indexName: `${tenantId}-${indexName}`,
        vectors: [
          {
            key: 'vec1',
            data: {
              float32: [1.0, 2.0, 3.0],
            },
            metadata: {
              category: 'test',
            },
          },
          {
            data: {
              float32: [4.0, 5.0, 6.0],
            },
            key: undefined,
          },
        ],
        vectorBucketName: vectorBucketS3,
      })
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/PutVectors',
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          vector: [
            {
              data: {
                float32: [1.0, 2.0, 3.0],
              },
            },
          ],
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should validate required fields', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/PutVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should validate vector data structure', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/PutVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          vector: [
            {
              data: {
                // missing float32
              },
            },
          ],
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should validate maxItems limit', async () => {
      const tooManyVectors = Array.from({ length: 501 }, (_, i) => ({
        data: {
          float32: [1.0, 2.0, 3.0],
        },
      }))

      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/PutVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          vector: tooManyVectors,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should handle non-existent index', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/PutVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: 'non-existent-index',
          vectors: [
            {
              data: {
                float32: [1.0, 2.0, 3.0],
              },
            },
          ],
        },
      })

      expect(response.statusCode).toBe(404)
    })
  })

  describe('POST /vector/QueryVectors', () => {
    let indexName: string

    beforeEach(async () => {
      indexName = `test-index-${Date.now()}`
      await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          dataType: 'float32',
          dimension: 3,
          distanceMetric: 'cosine',
          indexName: indexName,
          vectorBucketName: vectorBucketName,
        },
      })

      mockVectorStore.queryVectors.mockResolvedValue({
        vectors: [
          {
            key: 'vec1',
            distance: 0.95,
          },
        ],
      } as QueryVectorsOutput)
    })

    it('should query vector successfully', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/QueryVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          queryVector: {
            float32: [1.0, 2.0, 3.0],
          },
          topK: 10,
          returnDistance: true,
          returnMetadata: true,
        },
      })

      expect(response.statusCode).toBe(200)
      const body = JSON.parse(response.body)
      expect(body.vectors).toBeDefined()

      // Verify queryVectors was called with correct parameters
      expect(mockVectorStore.queryVectors).toHaveBeenCalledWith({
        vectorBucketName: vectorBucketS3,
        indexName: `${tenantId}-${indexName}`,
        indexArn: undefined,
        queryVector: {
          float32: [1.0, 2.0, 3.0],
        },
        topK: 10,
        returnDistance: true,
        returnMetadata: true,
        filter: undefined,
      })
    })

    it('should support metadata filtering', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/QueryVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          queryVector: {
            float32: [1.0, 2.0, 3.0],
          },
          topK: 5,
          filter: {
            category: 'test',
          },
        },
      })

      expect(response.statusCode).toBe(200)
      expect(mockVectorStore.queryVectors).toHaveBeenCalledWith(
        expect.objectContaining({
          filter: {
            category: 'test',
          },
        })
      )
    })

    it('should support complex logical filters', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/QueryVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          queryVector: {
            float32: [1.0, 2.0, 3.0],
          },
          topK: 5,
          filter: {
            $and: [{ category: 'test' }, { score: { $gt: 0.5 } }],
          },
        },
      })

      expect(response.statusCode).toBe(200)
      expect(mockVectorStore.queryVectors).toHaveBeenCalledWith(
        expect.objectContaining({
          filter: {
            $and: [{ category: 'test' }, { score: { $gt: 0.5 } }],
          },
        })
      )
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/QueryVectors',
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          queryVector: {
            float32: [1.0, 2.0, 3.0],
          },
          topK: 10,
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should validate required fields', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/QueryVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          queryVector: {
            float32: [1.0, 2.0, 3.0],
          },
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should validate queryVector structure', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/QueryVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          queryVector: {
            // missing float32
          },
          topK: 10,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should handle non-existent index', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/QueryVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: 'non-existent-index',
          queryVector: {
            float32: [1.0, 2.0, 3.0],
          },
          topK: 10,
        },
      })

      expect(response.statusCode).toBe(404)
    })
  })

  describe('POST /vector/DeleteVectors', () => {
    let indexName: string

    beforeEach(async () => {
      indexName = `test-index-${Date.now()}`
      await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          dataType: 'float32',
          dimension: 3,
          distanceMetric: 'cosine',
          indexName: indexName,
          vectorBucketName: vectorBucketName,
        },
      })

      mockVectorStore.deleteVectors.mockResolvedValue({} as DeleteVectorsOutput)
    })

    it('should delete vector successfully', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          keys: ['vec1', 'vec2', 'vec3'],
        },
      })

      expect(response.statusCode).toBe(200)

      // Verify deleteVectors was called with correct parameters
      expect(mockVectorStore.deleteVectors).toHaveBeenCalledWith({
        vectorBucketName: vectorBucketS3,
        indexName: `${tenantId}-${indexName}`,
        keys: ['vec1', 'vec2', 'vec3'],
      })
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteVectors',
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          keys: ['vec1'],
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should validate required fields', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should handle non-existent index', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/DeleteVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: 'non-existent-index',
          keys: ['vec1'],
        },
      })

      expect(response.statusCode).toBe(404)
    })
  })

  describe('POST /vector/ListVectors', () => {
    let indexName: string

    beforeEach(async () => {
      indexName = `test-index-${Date.now()}`
      await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          dataType: 'float32',
          dimension: 3,
          distanceMetric: 'cosine',
          indexName: indexName,
          vectorBucketName: vectorBucketName,
        },
      })

      mockVectorStore.listVectors.mockResolvedValue({
        vectors: [{ key: 'vec1' }, { key: 'vec2' }, { key: 'vec3' }],
        nextToken: undefined,
      } as ListVectorsOutput)
    })

    it('should list vector successfully', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
        },
      })

      expect(response.statusCode).toBe(200)
      const body = JSON.parse(response.body)
      expect(body.vectors).toBeDefined()
      expect(Array.isArray(body.vectors)).toBe(true)

      // Verify listVectors was called with correct parameters
      expect(mockVectorStore.listVectors).toHaveBeenCalledWith(
        expect.objectContaining({
          vectorBucketName: vectorBucketS3,
          indexName: `${tenantId}-${indexName}`,
        })
      )
    })

    it('should support maxResults parameter', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          maxResults: 10,
        },
      })

      expect(response.statusCode).toBe(200)
      expect(mockVectorStore.listVectors).toHaveBeenCalledWith(
        expect.objectContaining({
          maxResults: 10,
        })
      )
    })

    it('should support returnData and returnMetadata flags', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          returnData: true,
          returnMetadata: true,
        },
      })

      expect(response.statusCode).toBe(200)
      expect(mockVectorStore.listVectors).toHaveBeenCalledWith(
        expect.objectContaining({
          returnData: true,
          returnMetadata: true,
        })
      )
    })

    it('should support pagination with nextToken', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          nextToken: 'some-token',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(mockVectorStore.listVectors).toHaveBeenCalledWith(
        expect.objectContaining({
          nextToken: 'some-token',
        })
      )
    })

    it('should support segmentation parameters', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          segmentCount: 4,
          segmentIndex: 2,
        },
      })

      expect(response.statusCode).toBe(200)
      expect(mockVectorStore.listVectors).toHaveBeenCalledWith(
        expect.objectContaining({
          segmentCount: 4,
          segmentIndex: 2,
        })
      )
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectors',
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should validate required fields', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should validate maxResults range', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          maxResults: 501,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should validate segmentIndex range', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          segmentCount: 4,
          segmentIndex: 16,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should handle non-existent index', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/ListVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: 'non-existent-index',
        },
      })

      expect(response.statusCode).toBe(404)
    })
  })

  describe('POST /vector/GetVectors', () => {
    let indexName: string

    beforeEach(async () => {
      indexName = `test-index-${Date.now()}`
      await appInstance.inject({
        method: 'POST',
        url: '/vector/CreateIndex',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          dataType: 'float32',
          dimension: 3,
          distanceMetric: 'cosine',
          indexName: indexName,
          vectorBucketName: vectorBucketName,
        },
      })

      mockVectorStore.getVectors.mockResolvedValue({
        vectors: [
          {
            key: 'vec1',
            data: { float32: [1.0, 2.0, 3.0] },
            metadata: { category: 'test' },
          },
          {
            key: 'vec2',
            data: { float32: [4.0, 5.0, 6.0] },
            metadata: { category: 'test2' },
          },
        ],
        $metadata: {
          httpStatusCode: 200,
        },
      } as GetVectorsCommandOutput)
    })

    it('should get vector successfully', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          keys: ['vec1', 'vec2'],
          returnData: true,
          returnMetadata: true,
        },
      })

      expect(response.statusCode).toBe(200)
      const body = JSON.parse(response.body)
      expect(body.vectors).toBeDefined()
      expect(Array.isArray(body.vectors)).toBe(true)

      // Verify getVectors was called with correct parameters
      expect(mockVectorStore.getVectors).toHaveBeenCalledWith({
        vectorBucketName: vectorBucketS3,
        indexName: `${tenantId}-${indexName}`,
        keys: ['vec1', 'vec2'],
        returnData: true,
        returnMetadata: true,
      })
    })

    it('should work with default returnData and returnMetadata', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          keys: ['vec1'],
        },
      })

      expect(response.statusCode).toBe(200)
      expect(mockVectorStore.getVectors).toHaveBeenCalledWith(
        expect.objectContaining({
          returnData: false,
          returnMetadata: false,
        })
      )
    })

    it('should require authentication with service role', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetVectors',
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
          keys: ['vec1'],
        },
      })

      expect(response.statusCode).toBe(403)
    })

    it('should validate required fields', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: indexName,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('should handle non-existent index', async () => {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/vector/GetVectors',
        headers: {
          authorization: `Bearer ${serviceToken}`,
        },
        payload: {
          vectorBucketName: vectorBucketName,
          indexName: 'non-existent-index',
          keys: ['vec1'],
        },
      })

      expect(response.statusCode).toBe(404)
    })
  })
})
