import { getTenantConfig, multitenantKnex } from '@internal/database'
import { deriveVectorDatabaseUrl } from '@internal/database/vector-store-url'
import { ERRORS } from '@internal/errors'
import {
  BucketScopedSingleShard,
  KnexShardStoreFactory,
  ShardCatalog,
  Sharder,
  SingleShard,
} from '@internal/sharding'
import {
  createS3VectorClient,
  createVectorTransactionKnexResolver,
  KnexVectorMetadataDB,
  PgVectorStore,
  S3Vector,
  VectorStore,
  VectorStoreManager,
} from '@storage/protocols/vector'
import { FastifyInstance } from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import Knex from 'knex'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    s3Vector: VectorStoreManager
  }
}

export const s3vector = fastifyPlugin(async function (fastify: FastifyInstance) {
  const config = getConfig()
  const {
    vectorBucketProvider,
    vectorDatabaseCreate,
    vectorDatabaseURL,
    vectorS3Buckets,
    isMultitenant,
    databaseApplicationName,
  } = config

  // S3 mode: build a singleton client+adapter at boot.
  let s3Adapter: S3Vector | undefined
  if (vectorBucketProvider === 's3' && vectorS3Buckets.length > 0) {
    s3Adapter = new S3Vector(createS3VectorClient())
  }

  // pgvector + single-tenant: VECTOR_DATABASE_URL is the maintenance URL the
  // migration runner used to CREATE DATABASE; the runtime pool targets the
  // derived `storage_vectors` database on the same server. When
  // VECTOR_DATABASE_CREATE=false, the runtime pool targets VECTOR_DATABASE_URL
  // directly.
  let stPgVectorAdapter: PgVectorStore | undefined
  if (vectorBucketProvider === 'pgvector' && !isMultitenant && vectorDatabaseURL) {
    const connectionString = vectorDatabaseCreate
      ? deriveVectorDatabaseUrl(vectorDatabaseURL)
      : vectorDatabaseURL
    const vectorKnex = Knex({
      client: 'pg',
      connection: {
        connectionString,
        application_name: databaseApplicationName,
      },
      pool: { min: 0, max: 10 },
    })
    stPgVectorAdapter = new PgVectorStore(vectorKnex)
    fastify.addHook('onClose', async () => {
      await vectorKnex.destroy()
    })
  }

  const featureEnabled =
    (vectorBucketProvider === 's3' && Boolean(s3Adapter)) ||
    (vectorBucketProvider === 'pgvector' && (isMultitenant || Boolean(stPgVectorAdapter)))

  fastify.addHook('preHandler', async (req) => {
    if (!featureEnabled) {
      throw ERRORS.FeatureNotEnabled('vector', 'Vector service not configured')
    }

    const { vectorMaxBucketsCount, vectorMaxIndexesCount } = config

    let maxBucketCount = vectorMaxBucketsCount
    let maxIndexCount = vectorMaxIndexesCount

    if (isMultitenant) {
      const { features } = await getTenantConfig(req.tenantId)
      maxBucketCount = features?.vectorBuckets?.maxBuckets || vectorMaxBucketsCount
      maxIndexCount = features?.vectorBuckets?.maxIndexes || vectorMaxIndexesCount
    }

    const db = req.db.pool.acquire()
    const store = new KnexVectorMetadataDB(db)

    // Pick the adapter. In multi-tenant pgvector mode the adapter binds to the
    // request's own tenant pool (vectors live in the tenant DB); ST pgvector
    // uses the singleton; s3 uses the singleton S3 client.
    let adapter: VectorStore
    if (vectorBucketProvider === 'pgvector') {
      adapter = isMultitenant
        ? new PgVectorStore(createVectorTransactionKnexResolver(db))
        : stPgVectorAdapter!
    } else {
      adapter = s3Adapter!
    }

    let shard: Sharder
    if (vectorBucketProvider === 'pgvector') {
      shard = new BucketScopedSingleShard({
        keyPrefix: 'pgvector__',
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
