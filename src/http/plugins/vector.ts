import { getTenantConfig, multitenantPgExecutor } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { PgShardStoreFactory, ShardCatalog, SingleShard } from '@internal/sharding'
import {
  createS3VectorClient,
  PgVectorMetadataDB,
  S3Vector,
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

const s3VectorClient = createS3VectorClient()
const s3VectorAdapter = new S3Vector(s3VectorClient)

export const s3vector = fastifyPlugin(async function (fastify: FastifyInstance) {
  fastify.addHook('preHandler', async (req) => {
    const { isMultitenant, vectorS3Buckets, vectorMaxBucketsCount, vectorMaxIndexesCount } =
      getConfig()

    if (!vectorS3Buckets || vectorS3Buckets.length === 0) {
      throw ERRORS.FeatureNotEnabled('vector', 'Vector service not configured')
    }

    let maxBucketCount = vectorMaxBucketsCount
    let maxIndexCount = vectorMaxIndexesCount

    if (isMultitenant) {
      const { features } = await getTenantConfig(req.tenantId)
      maxBucketCount = features?.vectorBuckets?.maxBuckets || vectorMaxBucketsCount
      maxIndexCount = features?.vectorBuckets?.maxIndexes || vectorMaxIndexesCount
    }

    const store = new PgVectorMetadataDB(req.db)
    const shard = isMultitenant
      ? new ShardCatalog(new PgShardStoreFactory(multitenantPgExecutor))
      : new SingleShard({
          shardKey: vectorS3Buckets[0],
          capacity: 10000,
        })

    req.s3Vector = new VectorStoreManager(s3VectorAdapter, store, shard, {
      tenantId: req.tenantId,
      maxBucketCount,
      maxIndexCount,
    })
  })
})
