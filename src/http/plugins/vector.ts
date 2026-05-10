import { getTenantConfig, multitenantKnex } from '@internal/database'
import { ERRORS } from '@internal/errors'
import {
  BucketScopedSingleShard,
  KnexShardStoreFactory,
  ShardCatalog,
  Sharder,
  SingleShard,
} from '@internal/sharding'
import {
  createEmbeddedVectorStore,
  createS3VectorClient,
  EmbeddedVectorStore,
  KnexVectorMetadataDB,
  S3Vector,
  VectorStore,
  VectorStoreManager,
} from '@storage/protocols/vector'
import { FastifyInstance } from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    s3Vector: VectorStoreManager
  }
}

export const s3vector = fastifyPlugin(async function (fastify: FastifyInstance) {
  const config = getConfig()
  const { vectorBackend, vectorEmbeddedPath, vectorS3Buckets } = config

  let adapter: VectorStore | undefined
  let backendEnabled = false

  if (vectorBackend === 'embedded') {
    if (!vectorEmbeddedPath) {
      // Plugin still registers; preHandler will throw FeatureNotEnabled.
      backendEnabled = false
    } else {
      adapter = await createEmbeddedVectorStore({
        basePath: vectorEmbeddedPath,
        ttlMs: 60_000,
      })
      backendEnabled = true
    }
  } else if (vectorBackend === 's3') {
    if (vectorS3Buckets.length > 0) {
      adapter = new S3Vector(createS3VectorClient())
      backendEnabled = true
    }
  }

  fastify.addHook('preHandler', async (req) => {
    if (!backendEnabled || !adapter) {
      throw ERRORS.FeatureNotEnabled('vector', 'Vector service not configured')
    }

    const { isMultitenant, vectorMaxBucketsCount, vectorMaxIndexesCount } = config

    let maxBucketCount = vectorMaxBucketsCount
    let maxIndexCount = vectorMaxIndexesCount

    if (isMultitenant) {
      const { features } = await getTenantConfig(req.tenantId)
      maxBucketCount = features?.vectorBuckets?.maxBuckets || vectorMaxBucketsCount
      maxIndexCount = features?.vectorBuckets?.maxIndexes || vectorMaxIndexesCount
    }

    const db = req.db.pool.acquire()
    const store = new KnexVectorMetadataDB(db)

    let shard: Sharder
    if (vectorBackend === 'embedded') {
      shard = new BucketScopedSingleShard({
        keyPrefix: 'embedded__',
        capacity: Number.MAX_SAFE_INTEGER,
      })
    } else if (isMultitenant) {
      shard = new ShardCatalog(new KnexShardStoreFactory(multitenantKnex))
    } else {
      shard = new SingleShard({
        shardKey: vectorS3Buckets[0],
        capacity: 10000,
      })
    }

    req.s3Vector = new VectorStoreManager(adapter, store, shard, {
      tenantId: req.tenantId,
      maxBucketCount,
      maxIndexCount,
    })
  })
})

// Re-export so existing callers that pulled from this module keep compiling.
export type { EmbeddedVectorStore }
