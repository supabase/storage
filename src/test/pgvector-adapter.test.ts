import { PgVectorStore } from '@storage/protocols/vector'
import Knex from 'knex'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'

const TEST_DATABASE_URL =
  process.env.VECTOR_DATABASE_URL ||
  process.env.DATABASE_URL ||
  'postgresql://postgres:postgres@127.0.0.1/postgres'

function isTableAccessMethodLookup(sql: unknown): boolean {
  const text = String(sql)
  return text.includes('FROM pg_class') && text.includes('pg_am') && text.includes('relam')
}

function serializedRowsFromRawCall(call: unknown[]): Array<{
  key: string
  embedding: string
  metadata: Record<string, unknown>
}> {
  const params = call[1] as unknown[]
  return JSON.parse(params[1] as string)
}

function quoteIdent(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`
}

function qualifiedStorageVectorTable(table: string): string {
  return `${quoteIdent('storage_vectors')}.${quoteIdent(table)}`
}

function createManualUpsertDb(
  db: Knex.Knex,
  afterRaw: (callNo: number) => Promise<void>
): Knex.Knex {
  return {
    transaction: async (fn: (trx: Knex.Knex) => Promise<unknown>) =>
      db.transaction(async (trx) => {
        let callNo = 0
        const wrappedTrx = {
          raw: async (sql: string, bindings?: unknown[]) => {
            callNo += 1
            const result =
              bindings === undefined ? await trx.raw(sql) : await trx.raw(sql, bindings)
            await afterRaw(callNo)
            return result
          },
        } as unknown as Knex.Knex

        return fn(wrappedTrx)
      }),
  } as unknown as Knex.Knex
}

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

    await store
      .deleteVectorIndex({ vectorBucketName: bucket, indexName: index })
      .catch(() => undefined)
    await knex.destroy()
  })

  it('rejects maxResults=0 before querying Postgres', async () => {
    const raw = vi.fn()
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.listVectors({
        vectorBucketName: bucket,
        indexName: index,
        maxResults: 0,
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it('rejects maxResults above the pgvector list limit before querying Postgres', async () => {
    const raw = vi.fn()
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.listVectors({
        vectorBucketName: bucket,
        indexName: index,
        maxResults: 1001,
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it('uses the S3Vectors default page size when maxResults is omitted', async () => {
    const raw = vi.fn().mockResolvedValue({ rows: [] })
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.listVectors({
        vectorBucketName: bucket,
        indexName: index,
      })
    ).resolves.toEqual({
      vectors: [],
      nextToken: undefined,
    })

    expect(raw).toHaveBeenCalledTimes(1)
    expect(raw.mock.calls[0]?.[1]).toEqual([expect.any(String), 501])
  })

  it('rejects GetVectors requests above the S3Vectors key limit before querying Postgres', async () => {
    const raw = vi.fn()
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.getVectors({
        vectorBucketName: bucket,
        indexName: index,
        keys: Array.from({ length: 101 }, (_, i) => `key-${i}`),
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it('rejects GetVectors keys above the S3Vectors key length before querying Postgres', async () => {
    const raw = vi.fn().mockResolvedValue({ rows: [] })
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.getVectors({
        vectorBucketName: bucket,
        indexName: index,
        keys: ['x'.repeat(1025)],
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it('rejects DeleteVectors requests above the S3Vectors key limit before querying Postgres', async () => {
    const raw = vi.fn()
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.deleteVectors({
        vectorBucketName: bucket,
        indexName: index,
        keys: Array.from({ length: 501 }, (_, i) => `key-${i}`),
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it('rejects DeleteVectors keys above the S3Vectors key length before querying Postgres', async () => {
    const raw = vi.fn().mockResolvedValue({ rows: [] })
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.deleteVectors({
        vectorBucketName: bucket,
        indexName: index,
        keys: ['x'.repeat(1025)],
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it.each([
    -1, 0, 4001,
  ])('rejects invalid dimension=%s before creating pgvector tables', async (dimension) => {
    const raw = vi.fn()
    const transaction = vi.fn()
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await expect(
      localStore.createVectorIndex({
        vectorBucketName: bucket,
        indexName: index,
        dataType: 'float32',
        dimension,
        distanceMetric: 'cosine',
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
    expect(transaction).not.toHaveBeenCalled()
  })

  it('rejects PutVectors keys above the S3Vectors key length before writing to Postgres', async () => {
    const raw = vi.fn().mockResolvedValue({ rows: [] })
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: [
          {
            key: 'x'.repeat(1025),
            data: { float32: [1, 0] },
          },
        ],
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it('rejects PutVectors requests above the S3Vectors count limit before writing to Postgres', async () => {
    const raw = vi.fn().mockResolvedValue({ rows: [] })
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: Array.from({ length: 501 }, (_, i) => ({
          key: `vec-${i}`,
          data: { float32: [1, 0] },
        })),
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it('rejects nested metadata objects before writing to Postgres', async () => {
    const raw = vi.fn().mockResolvedValue({ rows: [] })
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: [
          {
            key: 'nested-metadata',
            data: { float32: [1, 0] },
            metadata: {
              nested: { value: 'not supported' },
            } as never,
          },
        ],
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it('allows list-valued metadata before writing to Postgres', async () => {
    const raw = vi.fn(async (sql: string) => {
      if (isTableAccessMethodLookup(sql)) {
        return { rows: [{ amname: 'heap' }] }
      }
      return { rows: [] }
    })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof raw }) => Promise<unknown>) =>
      fn({ raw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: [
          {
            key: 'list-metadata',
            data: { float32: [1, 0] },
            metadata: {
              tags: ['cats', 'docs', 2026, true],
            } as never,
          },
        ],
      })
    ).resolves.toEqual({})

    const insertCall = raw.mock.calls.find(([sql]) => !isTableAccessMethodLookup(sql))
    expect(insertCall).toBeDefined()
    expect(serializedRowsFromRawCall(insertCall!).at(0)?.metadata).toEqual({
      tags: ['cats', 'docs', 2026, true],
    })
  })

  it('rejects nested metadata arrays before writing to Postgres', async () => {
    const raw = vi.fn().mockResolvedValue({ rows: [] })
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: [
          {
            key: 'nested-list-metadata',
            data: { float32: [1, 0] },
            metadata: {
              tags: [['nested']],
            } as never,
          },
        ],
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it.each([
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
  ])('rejects non-finite metadata number %s before writing to Postgres', async (score) => {
    const raw = vi.fn().mockResolvedValue({ rows: [] })
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: [
          {
            key: 'non-finite-metadata',
            data: { float32: [1, 0] },
            metadata: { score } as never,
          },
        ],
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it('rejects duplicate put keys before regular upsert', async () => {
    const raw = vi.fn(async (sql: string) => {
      if (isTableAccessMethodLookup(sql)) {
        return { rows: [{ amname: 'heap' }] }
      }
      return { rows: [] }
    })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof raw }) => Promise<unknown>) =>
      fn({ raw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: [
          { key: 'dup', data: { float32: [1, 0] }, metadata: { version: 1 } },
          { key: 'other', data: { float32: [0, 1] }, metadata: { version: 1 } },
          { key: 'dup', data: { float32: [0.5, 0.5] }, metadata: { version: 2 } },
        ],
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
      message: 'Request must not contain duplicate keys',
    })

    expect(raw).not.toHaveBeenCalled()
  })

  it('rejects topK above the pgvector query limit before querying Postgres', async () => {
    const raw = vi.fn()
    const localStore = new PgVectorStore({ raw } as unknown as Knex.Knex)

    await expect(
      localStore.queryVectors({
        vectorBucketName: bucket,
        indexName: index,
        queryVector: { float32: [1, 0] },
        topK: 101,
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
    })
    expect(raw).not.toHaveBeenCalled()
  })

  it('allows topK at the S3Vectors query limit', async () => {
    const localBucket = `bucket-top-k-${Date.now()}-${Math.random()}`
    const localIndex = `index-top-k-${Date.now()}-${Math.random()}`
    const raw = vi.fn(async (sql: string, _params?: unknown[]) => {
      const text = String(sql)
      if (text.includes('FROM pg_index')) {
        return { rows: [{ opcname: 'halfvec_cosine_ops' }] }
      }
      if (isTableAccessMethodLookup(text)) {
        return { rows: [{ amname: 'heap' }] }
      }
      return { rows: [] }
    })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof raw }) => Promise<unknown>) =>
      fn({ raw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await expect(
      localStore.queryVectors({
        vectorBucketName: localBucket,
        indexName: localIndex,
        queryVector: { float32: [1, 0] },
        topK: 100,
      })
    ).resolves.toMatchObject({
      vectors: [],
    })

    const queryCall = raw.mock.calls.find(([sql]) => String(sql).includes('ORDER BY embedding'))
    expect(queryCall).toBeDefined()
    expect(queryCall![1]).toEqual(expect.arrayContaining([100]))
    expect(transaction).toHaveBeenCalledTimes(1)
    const efSearchCall = raw.mock.calls.find(([sql]) =>
      String(sql).includes("set_config('hnsw.ef_search'")
    )
    expect(efSearchCall?.[1]).toEqual(['100'])
  })

  it('sets hnsw ef_search to the default floor for standard scans below the default', async () => {
    const localBucket = `bucket-top-k-floor-${Date.now()}-${Math.random()}`
    const localIndex = `index-top-k-floor-${Date.now()}-${Math.random()}`
    const raw = vi.fn(async (sql: string, _params?: unknown[]) => {
      const text = String(sql)
      if (text.includes('FROM pg_index')) {
        return { rows: [{ opcname: 'halfvec_cosine_ops' }] }
      }
      if (isTableAccessMethodLookup(text)) {
        return { rows: [{ amname: 'heap' }] }
      }
      return { rows: [] }
    })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof raw }) => Promise<unknown>) =>
      fn({ raw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await expect(
      localStore.queryVectors({
        vectorBucketName: localBucket,
        indexName: localIndex,
        queryVector: { float32: [1, 0] },
        topK: 10,
      })
    ).resolves.toMatchObject({
      vectors: [],
    })

    expect(transaction).toHaveBeenCalledTimes(1)
    const efSearchCall = raw.mock.calls.find(([sql]) =>
      String(sql).includes("set_config('hnsw.ef_search'")
    )
    expect(efSearchCall?.[1]).toEqual(['40'])
    const queryCall = raw.mock.calls.find(([sql]) => String(sql).includes('ORDER BY embedding'))
    expect(queryCall?.[1]).toEqual(expect.arrayContaining([10]))
  })

  it('omits distance values when queryVectors has returnDistance=false', async () => {
    const localBucket = `bucket-no-distance-${Date.now()}-${Math.random()}`
    const localIndex = `index-no-distance-${Date.now()}-${Math.random()}`
    const raw = vi.fn(async (sql: string) => {
      const text = String(sql)
      if (text.includes('FROM pg_index')) {
        return { rows: [{ opcname: 'halfvec_l2_ops' }] }
      }
      if (isTableAccessMethodLookup(text)) {
        return { rows: [{ amname: 'heap' }] }
      }
      if (text.includes('ORDER BY embedding')) {
        return { rows: [{ key: 'a', distance: 123, metadata: { group: 'test' } }] }
      }
      return { rows: [] }
    })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof raw }) => Promise<unknown>) =>
      fn({ raw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    const result = await localStore.queryVectors({
      vectorBucketName: localBucket,
      indexName: localIndex,
      queryVector: { float32: [1, 0] },
      topK: 1,
      returnDistance: false,
      returnMetadata: true,
    })

    expect(result.distanceMetric).toBe('euclidean')
    expect(result.vectors).toEqual([
      {
        key: 'a',
        distance: undefined,
        metadata: { group: 'test' },
      },
    ])
    const queryCall = raw.mock.calls.find(([sql]) => String(sql).includes('ORDER BY embedding'))
    expect(String(queryCall?.[0])).not.toContain('AS distance')
  })

  it('omits distance values when queryVectors does not request returnDistance', async () => {
    const localBucket = `bucket-default-distance-${Date.now()}-${Math.random()}`
    const localIndex = `index-default-distance-${Date.now()}-${Math.random()}`
    const raw = vi.fn(async (sql: string) => {
      const text = String(sql)
      if (text.includes('FROM pg_index')) {
        return { rows: [{ opcname: 'halfvec_l2_ops' }] }
      }
      if (isTableAccessMethodLookup(text)) {
        return { rows: [{ amname: 'heap' }] }
      }
      if (text.includes('ORDER BY embedding')) {
        return { rows: [{ key: 'a', distance: 123, metadata: { group: 'test' } }] }
      }
      return { rows: [] }
    })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof raw }) => Promise<unknown>) =>
      fn({ raw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    const result = await localStore.queryVectors({
      vectorBucketName: localBucket,
      indexName: localIndex,
      queryVector: { float32: [1, 0] },
      topK: 1,
      returnMetadata: true,
    })

    expect(result.distanceMetric).toBe('euclidean')
    expect(result.vectors).toEqual([
      {
        key: 'a',
        distance: undefined,
        metadata: { group: 'test' },
      },
    ])
    const queryCall = raw.mock.calls.find(([sql]) => String(sql).includes('ORDER BY embedding'))
    expect(String(queryCall?.[0])).not.toContain('AS distance')
  })

  it('reuses the cached distance metric for repeated queries on the same index', async () => {
    const localBucket = `bucket-cache-${Date.now()}-${Math.random()}`
    const localIndex = `index-cache-${Date.now()}-${Math.random()}`
    const raw = vi.fn(async (sql: string) => {
      const text = String(sql)
      if (text.includes('FROM pg_index')) {
        return { rows: [{ opcname: 'halfvec_l2_ops' }] }
      }
      if (isTableAccessMethodLookup(text)) {
        return { rows: [{ amname: 'heap' }] }
      }
      return { rows: [] }
    })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof raw }) => Promise<unknown>) =>
      fn({ raw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)
    const command = {
      vectorBucketName: localBucket,
      indexName: localIndex,
      queryVector: { float32: [1, 0] },
      topK: 1,
    }

    await localStore.queryVectors(command)
    await localStore.queryVectors(command)

    const metricLookups = raw.mock.calls.filter(([sql]) => String(sql).includes('FROM pg_index'))
    const vectorQueries = raw.mock.calls.filter(([sql]) =>
      String(sql).includes('ORDER BY embedding')
    )
    expect(metricLookups).toHaveLength(1)
    expect(vectorQueries).toHaveLength(2)
    expect(vectorQueries.every(([sql]) => String(sql).includes('<->'))).toBe(true)
  })

  it('uses exact scan semantics and retries the OrioleDB probe after a transient probe failure', async () => {
    const localBucket = `bucket-probe-failure-${Date.now()}-${Math.random()}`
    const localIndex = `index-probe-failure-${Date.now()}-${Math.random()}`
    const trxRaw = vi.fn().mockResolvedValue({ rows: [] })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof trxRaw }) => Promise<void>) =>
      fn({ raw: trxRaw })
    )
    const raw = vi.fn(async (sql: string) => {
      const text = String(sql)
      if (text.includes('FROM pg_index')) {
        return { rows: [{ opcname: 'halfvec_cosine_ops' }] }
      }
      if (isTableAccessMethodLookup(text)) {
        const lookupCalls = raw.mock.calls.filter(([callSql]) =>
          isTableAccessMethodLookup(callSql)
        ).length
        if (lookupCalls === 1) {
          throw new Error('temporary probe failure')
        }
        return { rows: [{ amname: 'orioledb' }] }
      }
      if (text.includes('ORDER BY embedding')) {
        throw new Error('query should run through exact scan transaction')
      }
      return { rows: [] }
    })
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)
    const command = {
      vectorBucketName: localBucket,
      indexName: localIndex,
      queryVector: { float32: [1, 0] },
      topK: 1,
    }

    await localStore.queryVectors(command)
    await localStore.queryVectors(command)

    const capabilityLookups = raw.mock.calls.filter(([sql]) => isTableAccessMethodLookup(sql))
    expect(capabilityLookups).toHaveLength(2)
    expect(transaction).toHaveBeenCalledTimes(2)
    const exactScanSettings = trxRaw.mock.calls.filter(([sql]) =>
      String(sql).includes('enable_indexscan')
    )
    expect(exactScanSettings).toHaveLength(2)
    expect(exactScanSettings.every(([sql]) => String(sql).includes('enable_bitmapscan'))).toBe(true)
  })

  it('treats an empty table capability probe as unknown and does not cache it', async () => {
    const localBucket = `bucket-empty-probe-${Date.now()}-${Math.random()}`
    const localIndex = `index-empty-probe-${Date.now()}-${Math.random()}`
    const trxRaw = vi.fn().mockResolvedValue({ rows: [] })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof trxRaw }) => Promise<void>) =>
      fn({ raw: trxRaw })
    )
    const raw = vi.fn(async (sql: string) => {
      const text = String(sql)
      if (text.includes('FROM pg_index')) {
        return { rows: [{ opcname: 'halfvec_cosine_ops' }] }
      }
      if (isTableAccessMethodLookup(text)) {
        return { rows: [] }
      }
      if (text.includes('ORDER BY embedding')) {
        throw new Error('query should run through exact scan transaction')
      }
      return { rows: [] }
    })
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)
    const command = {
      vectorBucketName: localBucket,
      indexName: localIndex,
      queryVector: { float32: [1, 0] },
      topK: 1,
    }

    await localStore.queryVectors(command)
    await localStore.queryVectors(command)

    const capabilityLookups = raw.mock.calls.filter(([sql]) => isTableAccessMethodLookup(sql))
    expect(capabilityLookups).toHaveLength(2)
    expect(transaction).toHaveBeenCalledTimes(2)
    const exactScanSettings = trxRaw.mock.calls.filter(([sql]) =>
      String(sql).includes('enable_indexscan')
    )
    expect(exactScanSettings).toHaveLength(2)
    expect(exactScanSettings.every(([sql]) => String(sql).includes('enable_bitmapscan'))).toBe(true)
  })

  it('does not classify heap pools as bridged from an unexpected self-updated tuple error', async () => {
    const raw = vi.fn(async (sql: string) => {
      const text = String(sql)
      if (isTableAccessMethodLookup(text)) {
        return { rows: [{ amname: 'heap' }] }
      }
      if (text.includes('ON CONFLICT (key) DO UPDATE')) {
        throw new Error('unexpected self-updated tuple')
      }
      return { rows: [] }
    })
    const transaction = vi.fn()
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: [{ key: 'a', data: { float32: [1, 0] } }],
      })
    ).rejects.toThrow('unexpected self-updated tuple')

    expect(transaction).not.toHaveBeenCalled()
    expect(raw.mock.calls.filter(([sql]) => isTableAccessMethodLookup(sql))).toHaveLength(2)
  })

  it('does not re-probe table capability for unrelated regular upsert failures', async () => {
    const raw = vi.fn(async (sql: string) => {
      const text = String(sql)
      if (isTableAccessMethodLookup(text)) {
        return { rows: [{ amname: 'heap' }] }
      }
      if (text.includes('ON CONFLICT (key) DO UPDATE')) {
        throw new Error('different vector dimensions 2 and 3')
      }
      return { rows: [] }
    })
    const transaction = vi.fn()
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: [{ key: 'a', data: { float32: [1, 0] } }],
      })
    ).rejects.toThrow('different vector dimensions')

    expect(transaction).not.toHaveBeenCalled()
    expect(raw.mock.calls.filter(([sql]) => isTableAccessMethodLookup(sql))).toHaveLength(1)
  })

  it('uses actual table access method for OrioleDB bridged handling', async () => {
    const raw = vi.fn(async (sql: string) => {
      const text = String(sql)
      if (isTableAccessMethodLookup(text)) {
        return { rows: [{ amname: 'orioledb' }] }
      }
      if (text.includes('SHOW default_table_access_method')) {
        return { rows: [{ default_table_access_method: 'heap' }] }
      }
      if (text.includes('ON CONFLICT (key) DO UPDATE')) {
        throw new Error('should not use regular ON CONFLICT upsert')
      }
      return { rows: [] }
    })
    const trxRaw = vi.fn().mockResolvedValue({ rows: [] })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof trxRaw }) => Promise<void>) =>
      fn({ raw: trxRaw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await localStore.putVectors({
      vectorBucketName: bucket,
      indexName: index,
      vectors: [{ key: 'a', data: { float32: [1, 0] } }],
    })

    expect(raw.mock.calls.filter(([sql]) => isTableAccessMethodLookup(sql))).toHaveLength(1)
    expect(
      raw.mock.calls.some(([sql]) => String(sql).includes('SHOW default_table_access_method'))
    ).toBe(false)
    expect(transaction).toHaveBeenCalledTimes(1)
    expect(trxRaw).toHaveBeenCalledTimes(3)
    expect(String(trxRaw.mock.calls[1][0])).toContain('ON CONFLICT (key) DO NOTHING')

    await localStore.putVectors({
      vectorBucketName: bucket,
      indexName: index,
      vectors: [{ key: 'b', data: { float32: [0, 1] } }],
    })

    expect(raw.mock.calls.filter(([sql]) => isTableAccessMethodLookup(sql))).toHaveLength(1)
    expect(transaction).toHaveBeenCalledTimes(2)
  })

  it('rejects duplicate put keys before OrioleDB manual upsert', async () => {
    const raw = vi.fn(async (sql: string) => {
      if (isTableAccessMethodLookup(sql)) {
        return { rows: [{ amname: 'orioledb' }] }
      }
      return { rows: [] }
    })
    const trxRaw = vi.fn().mockResolvedValue({ rows: [] })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof trxRaw }) => Promise<void>) =>
      fn({ raw: trxRaw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: [
          { key: 'dup', data: { float32: [1, 0] }, metadata: { version: 1 } },
          { key: 'other', data: { float32: [0, 1] }, metadata: { version: 1 } },
          { key: 'dup', data: { float32: [0.5, 0.5] }, metadata: { version: 2 } },
        ],
      })
    ).rejects.toMatchObject({
      code: 'InvalidParameter',
      message: 'Request must not contain duplicate keys',
    })

    expect(raw).not.toHaveBeenCalled()
    expect(transaction).not.toHaveBeenCalled()
    expect(trxRaw).not.toHaveBeenCalled()
  })

  it('runs a final update after OrioleDB manual inserts to preserve concurrent upsert semantics', async () => {
    const raw = vi.fn(async (sql: string) => {
      if (isTableAccessMethodLookup(sql)) {
        return { rows: [{ amname: 'orioledb' }] }
      }
      return { rows: [] }
    })
    const trxRaw = vi.fn().mockResolvedValue({ rows: [] })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof trxRaw }) => Promise<void>) =>
      fn({ raw: trxRaw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await localStore.putVectors({
      vectorBucketName: bucket,
      indexName: index,
      vectors: [{ key: 'race-key', data: { float32: [1, 0] }, metadata: { writer: 'late' } }],
    })

    expect(trxRaw).toHaveBeenCalledTimes(3)
    expect(String(trxRaw.mock.calls[0][0])).toContain('UPDATE')
    expect(String(trxRaw.mock.calls[1][0])).toContain('ON CONFLICT (key) DO NOTHING')
    expect(String(trxRaw.mock.calls[2][0])).toContain('UPDATE')
  })

  it('sorts OrioleDB manual upsert rows by key before writing', async () => {
    const raw = vi.fn(async (sql: string) => {
      if (isTableAccessMethodLookup(sql)) {
        return { rows: [{ amname: 'orioledb' }] }
      }
      return { rows: [] }
    })
    const trxRaw = vi.fn().mockResolvedValue({ rows: [] })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof trxRaw }) => Promise<void>) =>
      fn({ raw: trxRaw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await localStore.putVectors({
      vectorBucketName: bucket,
      indexName: index,
      vectors: [
        { key: 'z', data: { float32: [1, 0] } },
        { key: 'a', data: { float32: [0, 1] } },
        { key: 'm', data: { float32: [0.5, 0.5] } },
      ],
    })

    const serializedRows = (trxRaw.mock.calls[0][1] as unknown[])[0] as string
    expect(JSON.parse(serializedRows).map((row: { key: string }) => row.key)).toEqual([
      'a',
      'm',
      'z',
    ])
  })

  it('retries OrioleDB manual upsert when Postgres reports a deadlock', async () => {
    const raw = vi.fn(async (sql: string) => {
      if (isTableAccessMethodLookup(sql)) {
        return { rows: [{ amname: 'orioledb' }] }
      }
      return { rows: [] }
    })
    const trxRaw = vi.fn().mockResolvedValue({ rows: [] })
    const deadlock = Object.assign(new Error('deadlock detected'), { code: '40P01' })
    const transaction = vi
      .fn()
      .mockRejectedValueOnce(deadlock)
      .mockImplementationOnce(async (fn: (trx: { raw: typeof trxRaw }) => Promise<void>) =>
        fn({ raw: trxRaw })
      )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: [{ key: 'retry-key', data: { float32: [1, 0] } }],
      })
    ).resolves.toEqual({})

    expect(transaction).toHaveBeenCalledTimes(2)
    expect(trxRaw).toHaveBeenCalledTimes(3)
  })

  it('preserves manual upsert semantics when two real transactions both miss a new key', async (ctx) => {
    if (!pgvectorAvailable) return ctx.skip()

    const manualStore = store as unknown as {
      putVectorsManually(db: Knex.Knex, table: string, serializedRows: string): Promise<void>
    }
    const manualTable = `manual_race_${Date.now()}_${Math.random().toString(16).slice(2)}`
    const qualified = qualifiedStorageVectorTable(manualTable)
    const firstWriterRows = JSON.stringify([
      {
        key: 'race-key',
        embedding: '[1,0]',
        metadata: { writer: 'first' },
      },
    ])
    const secondWriterRows = JSON.stringify([
      {
        key: 'race-key',
        embedding: '[0,1]',
        metadata: { writer: 'second' },
      },
    ])
    const secondWriterMissedInitialUpdate = Promise.withResolvers<void>()
    const allowSecondWriterToContinue = Promise.withResolvers<void>()

    await knex.raw(
      `CREATE TABLE storage_vectors.??
       (
         key text PRIMARY KEY,
         embedding halfvec(2) NOT NULL,
         metadata jsonb NOT NULL DEFAULT '{}'::jsonb
       )`,
      [manualTable]
    )

    try {
      const firstWriter = manualStore.putVectorsManually(
        createManualUpsertDb(knex, async (callNo) => {
          if (callNo === 1) {
            await secondWriterMissedInitialUpdate.promise
          }
        }),
        qualified,
        firstWriterRows
      )
      const secondWriter = manualStore.putVectorsManually(
        createManualUpsertDb(knex, async (callNo) => {
          if (callNo === 1) {
            secondWriterMissedInitialUpdate.resolve()
            await allowSecondWriterToContinue.promise
          }
        }),
        qualified,
        secondWriterRows
      )

      await secondWriterMissedInitialUpdate.promise
      await firstWriter
      allowSecondWriterToContinue.resolve()
      await secondWriter

      const result = await knex.raw(
        `SELECT embedding::text AS embedding, metadata
         FROM storage_vectors.??
         WHERE key = ?`,
        [manualTable, 'race-key']
      )

      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toMatchObject({
        embedding: '[0,1]',
        metadata: { writer: 'second' },
      })
    } finally {
      allowSecondWriterToContinue.resolve()
      secondWriterMissedInitialUpdate.resolve()
      await knex.raw('DROP TABLE IF EXISTS storage_vectors.??', [manualTable])
    }
  })

  it('falls back when bridged HNSW indexes reject ON CONFLICT DO UPDATE', async () => {
    const raw = vi.fn(async (sql: string) => {
      const text = String(sql)
      if (isTableAccessMethodLookup(text)) {
        const lookupCalls = raw.mock.calls.filter(([callSql]) =>
          isTableAccessMethodLookup(callSql)
        ).length
        if (lookupCalls === 1) {
          throw new Error('temporary probe failure')
        }
        return { rows: [{ amname: 'orioledb' }] }
      }
      if (text.includes('ON CONFLICT (key) DO UPDATE')) {
        throw new Error('unexpected self-updated tuple')
      }
      return { rows: [] }
    })
    const trxRaw = vi.fn().mockResolvedValue({ rows: [] })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof trxRaw }) => Promise<void>) =>
      fn({ raw: trxRaw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    await expect(
      localStore.putVectors({
        vectorBucketName: bucket,
        indexName: index,
        vectors: [
          { key: 'a', data: { float32: [1, 0] }, metadata: { category: 'cats' } },
          { key: 'b', data: { float32: [0, 1] }, metadata: { category: 'dogs' } },
        ],
      })
    ).resolves.toEqual({})

    expect(raw.mock.calls.filter(([sql]) => isTableAccessMethodLookup(sql))).toHaveLength(2)
    expect(transaction).toHaveBeenCalledTimes(1)
    expect(trxRaw).toHaveBeenCalledTimes(3)
    expect(String(trxRaw.mock.calls[1][0])).toContain('ON CONFLICT (key) DO NOTHING')

    await localStore.putVectors({
      vectorBucketName: bucket,
      indexName: index,
      vectors: [{ key: 'c', data: { float32: [1, 1] } }],
    })

    expect(raw.mock.calls.filter(([sql]) => isTableAccessMethodLookup(sql))).toHaveLength(2)
    expect(transaction).toHaveBeenCalledTimes(2)
  })

  it('does not let an older failed capability probe delete a newer cached probe', async () => {
    const isCapabilityLookup = (sql: unknown) =>
      String(sql).includes('JOIN pg_am am ON am.oid = c.relam')
    const firstLookup = Promise.withResolvers<{ rows: Array<{ amname: string }> }>()
    let lookupCount = 0
    const raw = vi.fn((sql: string) => {
      if (String(sql).includes('FROM pg_index')) {
        return Promise.resolve({ rows: [{ opcname: 'halfvec_cosine_ops' }] })
      }
      if (isCapabilityLookup(sql)) {
        lookupCount += 1
        if (lookupCount === 1) {
          return firstLookup.promise
        }
        return Promise.resolve({ rows: [{ amname: 'orioledb' }] })
      }
      return Promise.resolve({ rows: [] })
    })
    const trxRaw = vi.fn().mockResolvedValue({ rows: [] })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof trxRaw }) => Promise<unknown>) =>
      fn({ raw: trxRaw })
    )
    const localStore = new PgVectorStore({ raw, transaction } as unknown as Knex.Knex)

    const firstQuery = localStore.queryVectors({
      vectorBucketName: bucket,
      indexName: index,
      queryVector: { float32: [1, 0] },
      topK: 1,
    })
    await new Promise<void>((resolve) => setImmediate(resolve))
    expect(raw.mock.calls.filter(([sql]) => isCapabilityLookup(sql))).toHaveLength(1)

    await localStore.deleteVectorIndex({ vectorBucketName: bucket, indexName: index })
    await localStore.putVectors({
      vectorBucketName: bucket,
      indexName: index,
      vectors: [{ key: 'a', data: { float32: [1, 0] } }],
    })
    expect(raw.mock.calls.filter(([sql]) => isCapabilityLookup(sql))).toHaveLength(2)

    firstLookup.reject(new Error('temporary probe failure'))
    await expect(firstQuery).resolves.toMatchObject({ vectors: [] })

    await localStore.putVectors({
      vectorBucketName: bucket,
      indexName: index,
      vectors: [{ key: 'b', data: { float32: [0, 1] } }],
    })

    expect(raw.mock.calls.filter(([sql]) => isCapabilityLookup(sql))).toHaveLength(2)
  })

  it('creates an index through an active transaction resolver using a savepoint', async () => {
    const raw = vi.fn().mockResolvedValue({ rows: [] })
    const transaction = vi.fn(async (fn: (trx: { raw: typeof raw }) => Promise<unknown>) =>
      fn({ raw })
    )
    const localStore = new PgVectorStore({
      resolve: () =>
        ({
          isTransaction: true,
          transaction,
        }) as unknown as Knex.Knex,
    })

    await expect(
      localStore.createVectorIndex({
        vectorBucketName: bucket,
        indexName: index,
        dataType: 'float32',
        dimension: 4,
        distanceMetric: 'cosine',
      })
    ).resolves.toEqual({ $metadata: {} })

    expect(transaction).toHaveBeenCalledTimes(1)
    expect(raw).toHaveBeenCalledTimes(2)
    expect(String(raw.mock.calls[0][0])).toContain('CREATE TABLE')
    expect(String(raw.mock.calls[1][0])).toContain('CREATE INDEX')
  })

  it('invalidates a root capability cache after transaction-scoped index creation', async () => {
    const rootRaw = vi.fn(async (sql: string) => {
      if (isTableAccessMethodLookup(sql)) {
        return { rows: [{ amname: 'heap' }] }
      }
      return { rows: [] }
    })
    const rootDb = { raw: rootRaw } as unknown as Knex.Knex
    const rootStore = new PgVectorStore(rootDb)

    await rootStore.putVectors({
      vectorBucketName: bucket,
      indexName: index,
      vectors: [{ key: 'a', data: { float32: [1, 0] } }],
    })
    expect(rootRaw.mock.calls.filter(([sql]) => isTableAccessMethodLookup(sql))).toHaveLength(1)

    const transactionRaw = vi.fn().mockResolvedValue({ rows: [] })
    const transactionDb = {
      isTransaction: true,
      transaction: vi.fn(async (fn: (trx: { raw: typeof transactionRaw }) => Promise<unknown>) =>
        fn({ raw: transactionRaw })
      ),
    } as unknown as Knex.Knex
    const transactionStore = new PgVectorStore({
      resolve: () => transactionDb,
      root: () => rootDb,
    } as never)

    await transactionStore.createVectorIndex({
      vectorBucketName: bucket,
      indexName: index,
      dataType: 'float32',
      dimension: 4,
      distanceMetric: 'cosine',
    })

    await rootStore.putVectors({
      vectorBucketName: bucket,
      indexName: index,
      vectors: [{ key: 'b', data: { float32: [0, 1] } }],
    })

    expect(rootRaw.mock.calls.filter(([sql]) => isTableAccessMethodLookup(sql))).toHaveLength(2)
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
        {
          key: 'd',
          data: { float32: [0, 0, 0, 1] },
          metadata: { category: ['cats', 'birds'], score: 7 },
        },
        {
          key: 'e',
          data: { float32: [0.5, 0.5, 0, 0] },
          metadata: { category: 'numeric-list', score: [1, 5, 9] },
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
    function sortedVectorKeys(vectors: Array<{ key?: string }> | undefined): string[] {
      return (vectors ?? [])
        .map((v) => {
          if (v.key === undefined) {
            throw new Error('QueryVectors result is missing key')
          }
          return v.key
        })
        .sort()
    }

    const keys = sortedVectorKeys(filtered.vectors)
    expect(keys).toEqual(['a', 'c', 'd'])

    async function filteredKeys(filter: unknown): Promise<string[]> {
      const result = await store.queryVectors({
        vectorBucketName: bucket,
        indexName: index,
        queryVector: { float32: [1, 0, 0, 0] },
        topK: 5,
        filter: filter as never,
      })

      return sortedVectorKeys(result.vectors)
    }

    await expect(filteredKeys({ category: { $ne: 'cats' } })).resolves.toEqual(['b', 'e'])
    await expect(filteredKeys({ category: { $in: ['birds'] } })).resolves.toEqual(['d'])
    await expect(filteredKeys({ category: { $nin: ['cats', 'birds'] } })).resolves.toEqual([
      'b',
      'e',
    ])
    await expect(filteredKeys({ score: { $eq: 5 } })).resolves.toEqual(['a', 'e'])
    await expect(filteredKeys({ score: { $gt: 3 } })).resolves.toEqual(['a', 'c', 'd'])

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
    expect((list.vectors ?? []).map((v) => v.key).sort()).toEqual(['a', 'b', 'c', 'd', 'e'])

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
  })

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
  })

  it('finds the nearest vector inserted after HNSW index creation', async (ctx) => {
    if (!pgvectorAvailable) return ctx.skip()
    const exactScanIndex = `tenant-a-exact-scan-${Date.now()}`
    const distractors = Array.from({ length: 128 }, (_, i) => ({
      key: `far-${i.toString().padStart(3, '0')}`,
      data: { float32: [100 + i, -100 - i] },
    }))

    await store.createVectorIndex({
      vectorBucketName: bucket,
      indexName: exactScanIndex,
      dataType: 'float32',
      dimension: 2,
      distanceMetric: 'euclidean',
    })
    try {
      await store.putVectors({
        vectorBucketName: bucket,
        indexName: exactScanIndex,
        vectors: [
          ...distractors,
          {
            key: 'true-nearest-after-index',
            data: { float32: [0.001, 0] },
          },
        ],
      })

      const result = await store.queryVectors({
        vectorBucketName: bucket,
        indexName: exactScanIndex,
        queryVector: { float32: [0, 0] },
        topK: 1,
        returnDistance: true,
      })

      expect(result.distanceMetric).toBe('euclidean')
      expect(result.vectors?.map((v) => v.key)).toEqual(['true-nearest-after-index'])
      expect(result.vectors?.[0].distance).toBeCloseTo(0.001, 4)
    } finally {
      await store.deleteVectorIndex({ vectorBucketName: bucket, indexName: exactScanIndex })
    }
  })

  it('returns topK rows above the default HNSW ef_search on standard index scans', async (ctx) => {
    if (!pgvectorAvailable) return ctx.skip()

    const forcedKnex = Knex({
      client: 'pg',
      connection: { connectionString: TEST_DATABASE_URL, connectionTimeoutMillis: 5_000 },
      pool: { min: 0, max: 1 },
    })
    const forcedStore = new PgVectorStore(forcedKnex)
    const topKIndex = `tenant-a-topk-hnsw-${Date.now()}`
    const vectors = Array.from({ length: 150 }, (_, i) => ({
      key: `vec-${i.toString().padStart(3, '0')}`,
      data: { float32: [i + 1, 0] },
    }))

    try {
      await forcedKnex.raw('SET default_table_access_method = heap')
      await forcedStore.createVectorIndex({
        vectorBucketName: bucket,
        indexName: topKIndex,
        dataType: 'float32',
        dimension: 2,
        distanceMetric: 'euclidean',
      })
      await forcedKnex.raw('SET enable_seqscan = off')
      await forcedKnex.raw('SET hnsw.ef_search = 40')
      await forcedKnex.raw('SET hnsw.iterative_scan = off')
      await forcedStore.putVectors({
        vectorBucketName: bucket,
        indexName: topKIndex,
        vectors,
      })

      const result = await forcedStore.queryVectors({
        vectorBucketName: bucket,
        indexName: topKIndex,
        queryVector: { float32: [0, 0] },
        topK: 100,
      })

      expect(result.vectors).toHaveLength(100)
    } finally {
      try {
        await forcedStore.deleteVectorIndex({ vectorBucketName: bucket, indexName: topKIndex })
      } finally {
        await forcedKnex.destroy()
      }
    }
  })

  it('returns an empty list from an empty index with maxResults=1', async (ctx) => {
    if (!pgvectorAvailable) return ctx.skip()
    const emptyIndex = `tenant-a-empty-${Date.now()}`
    await store.createVectorIndex({
      vectorBucketName: bucket,
      indexName: emptyIndex,
      dataType: 'float32',
      dimension: 2,
      distanceMetric: 'cosine',
    })
    try {
      const result = await store.listVectors({
        vectorBucketName: bucket,
        indexName: emptyIndex,
        maxResults: 1,
      })

      expect(result.vectors).toEqual([])
      expect(result.nextToken).toBeUndefined()
    } finally {
      await store.deleteVectorIndex({ vectorBucketName: bucket, indexName: emptyIndex })
    }
  })

  it('returns an empty page when nextToken references a deleted vector with no later keys', async (ctx) => {
    if (!pgvectorAvailable) return ctx.skip()
    const deletedCursorIndex = `tenant-a-deleted-cursor-${Date.now()}`
    await store.createVectorIndex({
      vectorBucketName: bucket,
      indexName: deletedCursorIndex,
      dataType: 'float32',
      dimension: 2,
      distanceMetric: 'cosine',
    })
    try {
      await store.putVectors({
        vectorBucketName: bucket,
        indexName: deletedCursorIndex,
        vectors: [{ key: 'deleted-cursor', data: { float32: [1, 0] } }],
      })
      await store.deleteVectors({
        vectorBucketName: bucket,
        indexName: deletedCursorIndex,
        keys: ['deleted-cursor'],
      })

      const result = await store.listVectors({
        vectorBucketName: bucket,
        indexName: deletedCursorIndex,
        maxResults: 1,
        nextToken: 'deleted-cursor',
      })

      expect(result.vectors).toEqual([])
      expect(result.nextToken).toBeUndefined()
    } finally {
      await store.deleteVectorIndex({
        vectorBucketName: bucket,
        indexName: deletedCursorIndex,
      })
    }
  })

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
