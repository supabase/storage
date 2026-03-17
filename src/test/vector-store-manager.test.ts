import {
  ConflictException,
  CreateIndexCommandOutput,
  DeleteIndexCommandOutput,
  DeleteVectorsOutput,
  GetVectorsCommandOutput,
  ListVectorsOutput,
  PutVectorsOutput,
  QueryVectorsOutput,
} from '@aws-sdk/client-s3vectors'
import { ERRORS } from '@internal/errors'
import { Sharder } from '@internal/sharding'
import {
  KnexVectorMetadataDB,
  VECTOR_BUCKET_COUNT_LOCK,
  VectorLockResourceType,
  VectorMetadataDB,
  VectorStore,
  VectorStoreManager,
} from '@storage/protocols/vector'
import { VectorBucket } from '@storage/schemas'

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((res) => {
    resolve = res
  })

  return { promise, resolve }
}

function createVectorBucket(bucketName: string): VectorBucket {
  return {
    id: bucketName,
    created_at: new Date(),
    updated_at: new Date().toISOString(),
  } as unknown as VectorBucket
}
function createMockVectorStore(): jest.Mocked<VectorStore> {
  return {
    createVectorIndex: jest.fn().mockResolvedValue({} as CreateIndexCommandOutput),
    deleteVectorIndex: jest.fn().mockResolvedValue({} as DeleteIndexCommandOutput),
    putVectors: jest.fn().mockResolvedValue({} as PutVectorsOutput),
    listVectors: jest.fn().mockResolvedValue({} as ListVectorsOutput),
    queryVectors: jest.fn().mockResolvedValue({} as QueryVectorsOutput),
    deleteVectors: jest.fn().mockResolvedValue({} as DeleteVectorsOutput),
    getVectors: jest.fn().mockResolvedValue({} as GetVectorsCommandOutput),
  }
}

function createMockSharder(): jest.Mocked<Sharder> {
  return {
    createShard: jest.fn(),
    setShardStatus: jest.fn(),
    reserve: jest.fn(),
    confirm: jest.fn(),
    cancel: jest.fn(),
    expireLeases: jest.fn(),
    freeByLocation: jest.fn(),
    freeByResource: jest.fn(),
    shardStats: jest.fn(),
    findShardByResourceId: jest.fn(),
    listShardByKind: jest.fn(),
    withTnx: jest.fn(),
  } as unknown as jest.Mocked<Sharder>
}

function createMockVectorDb(): jest.Mocked<VectorMetadataDB> {
  return {
    withTransaction: jest.fn(),
    lockResource: jest.fn(),
    findVectorBucket: jest.fn(),
    createVectorBucket: jest.fn(),
    deleteVectorBucket: jest.fn(),
    listBuckets: jest.fn(),
    countBuckets: jest.fn(),
    countIndexes: jest.fn(),
    createVectorIndex: jest.fn(),
    getIndex: jest.fn(),
    listIndexes: jest.fn(),
    deleteVectorIndex: jest.fn(),
    findVectorIndexForBucket: jest.fn(),
  } as unknown as jest.Mocked<VectorMetadataDB>
}

function createDeterministicVectorDb(options: {
  bucketCount: number
  existingBuckets?: string[]
  onLockResource?: (
    resourceType: VectorLockResourceType,
    resourceId: string
  ) => Promise<void> | void
  onCreateVectorBucket?: (bucketName: string) => Promise<void> | void
  onDeleteVectorBucket?: (bucketName: string) => Promise<void> | void
}): VectorMetadataDB {
  const state = {
    bucketCount: options.bucketCount,
    existingBuckets: new Set(options.existingBuckets ?? []),
    countLockHeld: false,
    countLockWaiters: [] as Array<() => void>,
  }

  async function acquireCountLock() {
    if (!state.countLockHeld) {
      state.countLockHeld = true
      return
    }

    await new Promise<void>((resolve) => {
      state.countLockWaiters.push(resolve)
    })
  }

  function releaseCountLock() {
    const next = state.countLockWaiters.shift()
    if (next) {
      next()
      return
    }

    state.countLockHeld = false
  }

  return {
    async withTransaction<T>(fn: (db: KnexVectorMetadataDB) => T): Promise<T> {
      let holdsCountLock = false

      const tx: Partial<VectorMetadataDB> = {
        lockResource: async (resourceType: VectorLockResourceType, resourceId: string) => {
          await options.onLockResource?.(resourceType, resourceId)

          if (resourceType === 'global' && resourceId === VECTOR_BUCKET_COUNT_LOCK) {
            await acquireCountLock()
            holdsCountLock = true
          }
        },
        findVectorBucket: async (bucketName: string) => {
          if (state.existingBuckets.has(bucketName)) {
            return createVectorBucket(bucketName)
          }

          throw ERRORS.S3VectorNotFoundException('vector bucket', bucketName)
        },
        countBuckets: async () => state.bucketCount,
        createVectorBucket: async (bucketName: string) => {
          await options.onCreateVectorBucket?.(bucketName)

          if (state.existingBuckets.has(bucketName)) {
            throw new ConflictException({
              message: `vector bucket "${bucketName}" already exists`,
              $metadata: {},
            })
          }

          state.existingBuckets.add(bucketName)
          state.bucketCount += 1
        },
        listIndexes: async () => ({ indexes: [] }),
        deleteVectorBucket: async (bucketName: string) => {
          await options.onDeleteVectorBucket?.(bucketName)

          if (state.existingBuckets.delete(bucketName)) {
            state.bucketCount -= 1
          }
        },
      }

      try {
        return await fn(tx as KnexVectorMetadataDB)
      } finally {
        if (holdsCountLock) {
          releaseCountLock()
        }
      }
    },
    lockResource: async () => undefined,
    findVectorBucket: async () => {
      throw new Error('not implemented')
    },
    createVectorBucket: async () => undefined,
    deleteVectorBucket: async () => undefined,
    listBuckets: async () => ({ vectorBuckets: [] }),
    countBuckets: async () => state.bucketCount,
    countIndexes: async () => 0,
    createVectorIndex: async () => {
      throw new Error('not implemented')
    },
    getIndex: async () => {
      throw new Error('not implemented')
    },
    listIndexes: async () => ({ indexes: [] }),
    deleteVectorIndex: async () => undefined,
    findVectorIndexForBucket: async () => {
      throw new Error('not implemented')
    },
  }
}

describe('VectorStoreManager bucket lifecycle', () => {
  it('serializes concurrent creates for the final bucket slot', async () => {
    const releaseFirstCreate = deferred()
    const firstCreateStarted = deferred()

    const db = createDeterministicVectorDb({
      bucketCount: 1,
      existingBuckets: ['existing-bucket'],
      onCreateVectorBucket: async (bucketName) => {
        if (bucketName === 'bucket-a') {
          firstCreateStarted.resolve()
          await releaseFirstCreate.promise
        }
      },
    })

    const manager = new VectorStoreManager(createMockVectorStore(), db, createMockSharder(), {
      tenantId: 'test-tenant',
      maxBucketCount: 2,
      maxIndexCount: Infinity,
    })

    const createA = manager.createBucket('bucket-a')
    await firstCreateStarted.promise

    const createB = manager.createBucket('bucket-b')
    releaseFirstCreate.resolve()

    const results = await Promise.allSettled([createA, createB])

    expect(results).toEqual([
      { status: 'fulfilled', value: undefined },
      {
        status: 'rejected',
        reason: expect.objectContaining({ code: 'S3VectorMaxBucketsExceeded' }),
      },
    ])
  })

  it('keeps createBucket idempotent for an existing bucket even when at capacity', async () => {
    const db = createDeterministicVectorDb({
      bucketCount: 1,
      existingBuckets: ['bucket-a'],
    })

    const manager = new VectorStoreManager(createMockVectorStore(), db, createMockSharder(), {
      tenantId: 'test-tenant',
      maxBucketCount: 1,
      maxIndexCount: Infinity,
    })

    await expect(manager.createBucket('bucket-a')).resolves.toBeUndefined()
    await expect(manager.createBucket('bucket-b')).rejects.toMatchObject({
      code: 'S3VectorMaxBucketsExceeded',
    })
  })

  it('shares the bucket-count lock between delete and create so capacity is observed after delete commits', async () => {
    const releaseDelete = deferred()
    const deleteReachedRemoval = deferred()

    const db = createDeterministicVectorDb({
      bucketCount: 1,
      existingBuckets: ['bucket-to-delete'],
      onDeleteVectorBucket: async (bucketName) => {
        if (bucketName === 'bucket-to-delete') {
          deleteReachedRemoval.resolve()
          await releaseDelete.promise
        }
      },
    })

    const manager = new VectorStoreManager(createMockVectorStore(), db, createMockSharder(), {
      tenantId: 'test-tenant',
      maxBucketCount: 1,
      maxIndexCount: Infinity,
    })

    const deletePromise = manager.deleteBucket('bucket-to-delete')
    await deleteReachedRemoval.promise

    const createPromise = manager.createBucket('bucket-new')
    releaseDelete.resolve()

    await expect(deletePromise).resolves.toBeUndefined()
    await expect(createPromise).resolves.toBeUndefined()
  })

  it('does not block unrelated creates while delete waits on the target bucket lock', async () => {
    const releaseBucketLock = deferred()
    const deleteWaitingOnBucketLock = deferred()

    const db = createDeterministicVectorDb({
      bucketCount: 1,
      existingBuckets: ['bucket-a'],
      onLockResource: async (resourceType, resourceId) => {
        if (resourceType === 'bucket' && resourceId === 'bucket-a') {
          deleteWaitingOnBucketLock.resolve()
          await releaseBucketLock.promise
        }
      },
    })

    const manager = new VectorStoreManager(createMockVectorStore(), db, createMockSharder(), {
      tenantId: 'test-tenant',
      maxBucketCount: 2,
      maxIndexCount: Infinity,
    })

    const deletePromise = manager.deleteBucket('bucket-a')
    await deleteWaitingOnBucketLock.promise

    await expect(manager.createBucket('bucket-b')).resolves.toBeUndefined()

    releaseBucketLock.resolve()
    await expect(deletePromise).resolves.toBeUndefined()
  })

  it('takes the per-bucket lock before the global count lock during bucket deletion', async () => {
    const callOrder: string[] = []
    const db = createDeterministicVectorDb({
      bucketCount: 1,
      existingBuckets: ['bucket-a'],
      onLockResource: (resourceType, resourceId) => {
        callOrder.push(`${resourceType}:${resourceId}`)
      },
      onDeleteVectorBucket: () => {
        callOrder.push('delete')
      },
    })

    const manager = new VectorStoreManager(createMockVectorStore(), db, createMockSharder(), {
      tenantId: 'test-tenant',
      maxBucketCount: Infinity,
      maxIndexCount: Infinity,
    })

    await manager.deleteBucket('bucket-a')

    expect(callOrder).toEqual(['bucket:bucket-a', `global:${VECTOR_BUCKET_COUNT_LOCK}`, 'delete'])
  })

  it('re-checks bucket existence after taking the bucket lock before creating an index', async () => {
    const db = createMockVectorDb()
    const sharder = createMockSharder()
    const vectorStore = createMockVectorStore()

    db.findVectorBucket
      .mockResolvedValueOnce(createVectorBucket('bucket-a'))
      .mockRejectedValueOnce(ERRORS.S3VectorNotFoundException('vector bucket', 'bucket-a'))
    db.withTransaction.mockImplementation(async (fn) => fn(db as unknown as KnexVectorMetadataDB))

    const manager = new VectorStoreManager(vectorStore, db, sharder, {
      tenantId: 'test-tenant',
      maxBucketCount: Infinity,
      maxIndexCount: Infinity,
    })

    await expect(
      manager.createVectorIndex({
        dataType: 'float32',
        dimension: 4,
        distanceMetric: 'cosine',
        indexName: 'index-a',
        vectorBucketName: 'bucket-a',
      })
    ).rejects.toMatchObject({ code: 'NotFoundException' })

    expect(db.lockResource).toHaveBeenCalledWith('bucket', 'bucket-a')
    expect(db.findVectorBucket).toHaveBeenCalledTimes(2)
    expect(db.countIndexes).not.toHaveBeenCalled()
    expect(db.createVectorIndex).not.toHaveBeenCalled()
    expect(sharder.reserve).not.toHaveBeenCalled()
    expect(vectorStore.createVectorIndex).not.toHaveBeenCalled()
  })
})
