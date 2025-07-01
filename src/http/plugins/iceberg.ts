import fastifyPlugin from 'fastify-plugin'
import { FastifyInstance } from 'fastify'
import { getConfig } from '../../config'
import { RestCatalogTenant } from '@storage/protocols/iceberg/catalog/rest-tenant-proxy'
import { KnexMetastore } from '@storage/protocols/iceberg/knex'
import { multitenantKnex } from '@internal/database'

declare module 'fastify' {
  interface FastifyRequest {
    isIcebergBucket?: boolean
    internalIcebergBucketName?: string
    internalIcebergNamespaceId?: string
    icebergCatalog?: RestCatalogTenant
  }
}

const { icebergWarehouse, icebergCatalogUrl, storageS3Region, isMultitenant, encryptionKey } =
  getConfig()

export const icebergRestCatalog = fastifyPlugin(async function (fastify: FastifyInstance) {
  fastify.addHook('preHandler', async (req) => {
    req.icebergCatalog = new RestCatalogTenant({
      restCatalogUrl: icebergCatalogUrl,
      region: storageS3Region,
      warehouse: icebergWarehouse,
      tenantId: req.tenantId,
      signatureSecret: encryptionKey,
      metastore: new KnexMetastore(isMultitenant ? multitenantKnex : req.db.pool.acquire(), {
        storeTenantId: isMultitenant,
        schema: isMultitenant ? 'public' : 'storage',
      }),
    })
  })
})

export const detectS3IcebergBucket = fastifyPlugin(
  async function (fastify: FastifyInstance) {
    fastify.addHook('preHandler', async (req) => {
      const params = req.params as { Bucket?: string }
      if (!params.Bucket) {
        return
      }

      const isIcebergBucket = params.Bucket.includes('--iceberg')

      if (!isIcebergBucket) return

      const bucketParts = params.Bucket.split('--iceberg')

      if (bucketParts.length < 1) {
        return
      }

      const tableId = bucketParts[0]

      try {
        const table = await req.icebergCatalog?.findTableById({
          id: tableId,
          tenantId: req.tenantId,
        })

        if (!table) {
          return
        }

        req.isIcebergBucket = true
        req.internalIcebergBucketName = table.location.replace('s3://', '')
        req.internalIcebergNamespaceId = table.namespace_id
      } catch {
        return
      }
    })
  },
  { name: 'iceberg-bucket' }
)
