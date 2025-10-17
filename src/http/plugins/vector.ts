import fastifyPlugin from 'fastify-plugin'
import { FastifyInstance } from 'fastify'
import { getTenantConfig } from '@internal/database'
import {
  createS3VectorClient,
  KnexVectorMetadataDB,
  VectorStoreManager,
  S3Vector,
} from '@storage/protocols/vector'
import { getConfig } from '../../config'
import { ERRORS } from '@internal/errors'

declare module 'fastify' {
  interface FastifyRequest {
    s3Vector: VectorStoreManager
  }
}

const s3VectorClient = createS3VectorClient()
const s3VectorAdapter = new S3Vector(s3VectorClient)

export const s3vector = fastifyPlugin(async function (fastify: FastifyInstance) {
  fastify.addHook('preHandler', async (req) => {
    const { isMultitenant, vectorBucketS3, vectorMaxBucketsCount, vectorMaxIndexesCount } =
      getConfig()

    if (!vectorBucketS3) {
      throw ERRORS.FeatureNotEnabled('vector', 'Vector service not configured')
    }

    let maxBucketCount = vectorMaxBucketsCount
    let maxIndexCount = vectorMaxIndexesCount

    if (isMultitenant) {
      const { features } = await getTenantConfig(req.tenantId)
      maxBucketCount = features?.vectorBuckets?.maxBuckets || vectorMaxBucketsCount
      maxIndexCount = features?.vectorBuckets?.maxIndexes || vectorMaxIndexesCount
    }

    const db = req.db.pool.acquire()
    const store = new KnexVectorMetadataDB(db)
    req.s3Vector = new VectorStoreManager(s3VectorAdapter, store, {
      tenantId: req.tenantId,
      vectorBucketName: vectorBucketS3,
      maxBucketCount: maxBucketCount,
      maxIndexCount: maxIndexCount,
    })
  })
})
