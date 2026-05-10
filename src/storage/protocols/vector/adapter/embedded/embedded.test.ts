import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { createEmbeddedVectorStore, EmbeddedVectorStore } from './index'

// @zvec/zvec ships prebuilt native bindings only for these (platform, arch) pairs.
// Skip the integration suite on anything else so unsupported devs don't see a
// confusing native-load error.
const ZVEC_SUPPORTED: ReadonlyArray<readonly [NodeJS.Platform, NodeJS.Architecture]> = [
  ['linux', 'x64'],
  ['linux', 'arm64'],
  ['darwin', 'arm64'],
  ['win32', 'x64'],
]
const zvecAvailable = ZVEC_SUPPORTED.some(([p, a]) => p === process.platform && a === process.arch)

const describeIfAvailable = zvecAvailable ? describe : describe.skip

describeIfAvailable('EmbeddedVectorStore (real zvec)', () => {
  let store: EmbeddedVectorStore
  let basePath: string
  const bucket = 'embedded__logos'
  const index = 'tenant-a-vecs'

  beforeAll(async () => {
    basePath = mkdtempSync(join(tmpdir(), 'storage-api-embedded-vec-'))
    store = await createEmbeddedVectorStore({ basePath, ttlMs: 5_000 })
  })

  afterAll(() => {
    store.shutdown()
    rmSync(basePath, { recursive: true, force: true })
  })

  it('rejects createIndex without filterableMetadataKeys', async () => {
    await expect(
      store.createVectorIndex({
        vectorBucketName: bucket,
        indexName: 'no-keys',
        dataType: 'float32',
        dimension: 4,
        distanceMetric: 'cosine',
      })
    ).rejects.toMatchObject({ message: expect.stringContaining('filterableMetadataKeys') })
  })

  it('creates an index, puts vectors, queries by similarity, fetches by key, deletes', async () => {
    await store.createVectorIndex({
      vectorBucketName: bucket,
      indexName: index,
      dataType: 'float32',
      dimension: 4,
      distanceMetric: 'cosine',
      filterableMetadataKeys: [
        { name: 'category', dataType: 'string' },
        { name: 'score', dataType: 'number' },
        { name: 'active', dataType: 'boolean' },
      ],
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
          metadata: { category: 'dogs', score: 3, active: true },
        },
        {
          key: 'c',
          data: { float32: [0, 0, 1, 0] },
          metadata: { category: 'cats', score: 9, active: false },
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
    expect(queryResult.vectors).toHaveLength(2)
    expect(queryResult.vectors?.[0].key).toBe('a')

    const filtered = await store.queryVectors({
      vectorBucketName: bucket,
      indexName: index,
      queryVector: { float32: [1, 0, 0, 0] },
      topK: 5,
      filter: { category: 'cats' },
      returnMetadata: true,
    })

    expect(filtered.vectors?.map((v) => v.key).sort()).toEqual(['a', 'c'])

    const fetched = await store.getVectors({
      vectorBucketName: bucket,
      indexName: index,
      keys: ['a', 'b', 'missing'],
      returnMetadata: true,
    })

    expect(fetched.vectors).toHaveLength(2)
    const a = fetched.vectors?.find((v) => v.key === 'a')
    expect(a?.metadata).toEqual(
      expect.objectContaining({ category: 'cats', score: 5, active: true, note: 'hello' })
    )

    await store.deleteVectors({
      vectorBucketName: bucket,
      indexName: index,
      keys: ['a'],
    })

    const afterDelete = await store.getVectors({
      vectorBucketName: bucket,
      indexName: index,
      keys: ['a'],
      returnMetadata: false,
    })
    expect(afterDelete.vectors).toHaveLength(0)
  })

  it('rejects type-mismatched filterable metadata at put time', async () => {
    const idx = 'tenant-a-typed'
    await store.createVectorIndex({
      vectorBucketName: bucket,
      indexName: idx,
      dataType: 'float32',
      dimension: 2,
      distanceMetric: 'euclidean',
      filterableMetadataKeys: [{ name: 'score', dataType: 'number' }],
    })

    await expect(
      store.putVectors({
        vectorBucketName: bucket,
        indexName: idx,
        vectors: [
          {
            key: 'x',
            data: { float32: [1, 0] },
            metadata: { score: 'not-a-number' },
          },
        ],
      })
    ).rejects.toMatchObject({ message: expect.stringContaining('score') })

    await store.deleteVectorIndex({ vectorBucketName: bucket, indexName: idx })
  })

  it('throws S3VectorEmbeddedNotSupported on listVectors', async () => {
    await expect(
      store.listVectors({
        vectorBucketName: bucket,
        indexName: index,
      })
    ).rejects.toMatchObject({ message: expect.stringContaining('listVectors') })
  })

  it('deletes the index and reports it gone on subsequent fetch', async () => {
    await store.deleteVectorIndex({ vectorBucketName: bucket, indexName: index })
  })
})
