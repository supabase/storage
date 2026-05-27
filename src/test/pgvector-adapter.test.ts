import { PgVectorStore } from '@storage/protocols/vector'
import Knex from 'knex'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

const TEST_DATABASE_URL =
  process.env.VECTOR_DATABASE_URL ||
  process.env.DATABASE_URL ||
  'postgresql://postgres:postgres@127.0.0.1/postgres'

// pgvector availability is probed in beforeAll. We can't probe at module load
// because the project builds to CJS, where top-level await is unsupported.
// Tests guard at the top of each `it` body and skip when unavailable.
let pgvectorAvailable = false

describe('PgVectorStore (real pgvector)', () => {
  let store: PgVectorStore
  let knex: Knex.Knex
  const bucket = 'pgvector__logos'
  const index = 'tenant-a-vecs'

  beforeAll(async () => {
    const probe = Knex({
      client: 'pg',
      connection: { connectionString: TEST_DATABASE_URL, connectionTimeoutMillis: 2_000 },
      pool: { min: 0, max: 1 },
    })
    try {
      await probe.raw('CREATE EXTENSION IF NOT EXISTS vector')
      await probe.raw('CREATE SCHEMA IF NOT EXISTS storage_vectors')
      pgvectorAvailable = true
    } catch (e) {
      console.warn(
        '[pgvector.test] skipping: pgvector unavailable on test DB:',
        (e as Error).message
      )
    } finally {
      await probe.destroy()
    }

    if (!pgvectorAvailable) return

    knex = Knex({
      client: 'pg',
      connection: { connectionString: TEST_DATABASE_URL, connectionTimeoutMillis: 5_000 },
      pool: { min: 0, max: 4 },
    })
    store = new PgVectorStore(knex)
  })

  afterAll(async () => {
    if (!pgvectorAvailable) return
    try {
      await store.deleteVectorIndex({ vectorBucketName: bucket, indexName: index })
    } catch {
      /* swallow cleanup errors */
    }
    await knex.destroy()
  })

  it('creates an index, puts vectors, queries, filters, fetches, lists, deletes', async (ctx) => {
    if (!pgvectorAvailable) return ctx.skip()
    await store.createVectorIndex({
      vectorBucketName: bucket,
      indexName: index,
      dataType: 'float32',
      dimension: 4,
      distanceMetric: 'cosine',
    })

    await store.putVectors({
      vectorBucketName: bucket,
      indexName: index,
      vectors: [
        {
          key: 'a',
          data: { float32: [1, 0, 0, 0] },
          metadata: { category: 'cats', score: 5, active: true, note: 'hello' },
        },
        {
          key: 'b',
          data: { float32: [0, 1, 0, 0] },
          metadata: { category: 'dogs', score: 3 },
        },
        {
          key: 'c',
          data: { float32: [0, 0, 1, 0] },
          metadata: { category: 'cats', score: 9 },
        },
      ],
    })

    const queryResult = await store.queryVectors({
      vectorBucketName: bucket,
      indexName: index,
      queryVector: { float32: [1, 0, 0, 0] },
      topK: 2,
      returnDistance: true,
      returnMetadata: true,
    })
    expect(queryResult.distanceMetric).toBe('cosine')
    expect(queryResult.vectors?.[0].key).toBe('a')
    expect(queryResult.vectors).toHaveLength(2)

    const filtered = await store.queryVectors({
      vectorBucketName: bucket,
      indexName: index,
      queryVector: { float32: [1, 0, 0, 0] },
      topK: 5,
      filter: { category: 'cats' } as never,
      returnMetadata: true,
    })
    const keys = (filtered.vectors ?? []).map((v) => v.key).sort()
    expect(keys).toEqual(['a', 'c'])

    const fetched = await store.getVectors({
      vectorBucketName: bucket,
      indexName: index,
      keys: ['a', 'b', 'missing'],
      returnMetadata: true,
    })
    expect(fetched.vectors).toHaveLength(2)
    const a = fetched.vectors?.find((v) => v.key === 'a')
    expect(a?.metadata).toMatchObject({ category: 'cats', score: 5, active: true, note: 'hello' })

    const list = await store.listVectors({
      vectorBucketName: bucket,
      indexName: index,
      maxResults: 100,
    })
    expect((list.vectors ?? []).map((v) => v.key).sort()).toEqual(['a', 'b', 'c'])

    const firstPage = await store.listVectors({
      vectorBucketName: bucket,
      indexName: index,
      maxResults: 2,
    })
    expect(firstPage.vectors).toHaveLength(2)
    expect(firstPage.nextToken).toBeDefined()

    const secondPage = await store.listVectors({
      vectorBucketName: bucket,
      indexName: index,
      maxResults: 2,
      nextToken: firstPage.nextToken,
    })
    expect((secondPage.vectors ?? []).length).toBeGreaterThan(0)

    await store.deleteVectors({
      vectorBucketName: bucket,
      indexName: index,
      keys: ['a'],
    })
    const afterDel = await store.getVectors({
      vectorBucketName: bucket,
      indexName: index,
      keys: ['a'],
    })
    expect(afterDel.vectors).toHaveLength(0)
  }, 20_000)

  it('ranks results by L2 distance when distanceMetric is euclidean', async (ctx) => {
    if (!pgvectorAvailable) return ctx.skip()
    const euclIndex = 'tenant-a-vecs-euclidean'
    await store.createVectorIndex({
      vectorBucketName: bucket,
      indexName: euclIndex,
      dataType: 'float32',
      dimension: 4,
      distanceMetric: 'euclidean',
    })
    try {
      await store.putVectors({
        vectorBucketName: bucket,
        indexName: euclIndex,
        vectors: [
          { key: 'near', data: { float32: [1, 0, 0, 0] } },
          { key: 'mid', data: { float32: [0.5, 0.5, 0, 0] } },
          { key: 'far', data: { float32: [0, 0, 0, 1] } },
        ],
      })

      const result = await store.queryVectors({
        vectorBucketName: bucket,
        indexName: euclIndex,
        queryVector: { float32: [1, 0, 0, 0] },
        topK: 3,
        returnDistance: true,
      })
      expect(result.distanceMetric).toBe('euclidean')
      const keys = (result.vectors ?? []).map((v) => v.key)
      // L2 from [1,0,0,0]: near=0, mid≈0.707, far≈√2 — strict ordering.
      expect(keys).toEqual(['near', 'mid', 'far'])
      expect(result.vectors?.[0].distance).toBeCloseTo(0, 3)
    } finally {
      await store.deleteVectorIndex({ vectorBucketName: bucket, indexName: euclIndex })
    }
  }, 20_000)

  it('returns NotFound when querying a missing index', async (ctx) => {
    if (!pgvectorAvailable) return ctx.skip()
    await expect(
      store.queryVectors({
        vectorBucketName: bucket,
        indexName: 'does-not-exist',
        queryVector: { float32: [1, 0, 0, 0] },
        topK: 1,
      })
    ).rejects.toMatchObject({ code: 'NotFoundException' })
  })

  it('deleteVectorIndex is idempotent on missing tables', async (ctx) => {
    if (!pgvectorAvailable) return ctx.skip()
    await expect(
      store.deleteVectorIndex({ vectorBucketName: bucket, indexName: 'never-created' })
    ).resolves.toBeDefined()
  })
})
