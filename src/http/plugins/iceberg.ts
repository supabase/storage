import fastifyPlugin from 'fastify-plugin'
import { FastifyInstance } from 'fastify'
import { KnexMetastore, TableIndex } from '@storage/protocols/iceberg/knex'
import { getTenantConfig, multitenantKnex } from '@internal/database'
import { getCatalogAuthStrategy, TenantAwareRestCatalog } from '@storage/protocols/iceberg/catalog'
import { getConfig } from '../../config'
import { ICEBERG_BUCKET_RESERVED_SUFFIX } from '@storage/limits'
import { KnexShardStoreFactory, ShardCatalog, SingleShard } from '@internal/sharding'

declare module 'fastify' {
  interface FastifyRequest {
    isIcebergBucket?: boolean
    internalIcebergBucketName?: string
    internalIcebergNamespaceId?: string
    icebergCatalog?: TenantAwareRestCatalog
  }
}

const {
  icebergBucketDetectionMode,
  icebergMaxNamespaceCount,
  icebergMaxTableCount,
  icebergMaxCatalogsCount,
  icebergCatalogAuthType,
  icebergWarehouse,
  icebergCatalogUrl,
  isMultitenant,
} = getConfig()

const catalogAuthType = getCatalogAuthStrategy(icebergCatalogAuthType)

export const icebergRestCatalog = fastifyPlugin(async function (fastify: FastifyInstance) {
  fastify.addHook('preHandler', async (req) => {
    const limits = {
      maxCatalogsCount: icebergMaxCatalogsCount,
      maxNamespaceCount: icebergMaxNamespaceCount,
      maxTableCount: icebergMaxTableCount,
    }

    if (isMultitenant) {
      const { features } = await getTenantConfig(req.tenantId)

      limits.maxTableCount = features.icebergCatalog.maxTables
      limits.maxNamespaceCount = features.icebergCatalog.maxNamespaces
      limits.maxCatalogsCount = features.icebergCatalog.maxCatalogs
    }

    req.icebergCatalog = new TenantAwareRestCatalog({
      tenantId: req.tenantId,
      limits,
      restCatalogUrl: icebergCatalogUrl,
      auth: catalogAuthType,
      sharding: isMultitenant
        ? new ShardCatalog(new KnexShardStoreFactory(multitenantKnex))
        : new SingleShard({
            shardKey: icebergWarehouse,
            capacity: 10000,
          }),
      metastore: new KnexMetastore(isMultitenant ? multitenantKnex : req.db.pool.acquire(), {
        multiTenant: isMultitenant,
        schema: isMultitenant ? 'public' : 'storage',
      }),
    })
  })
})

export const detectS3IcebergBucket = fastifyPlugin(
  async function (fastify: FastifyInstance) {
    fastify.addHook('preHandler', async (req) => {
      const params = req.params as { Bucket?: string; '*'?: string }
      if (!params.Bucket) {
        return
      }

      const isIcebergBucket = params.Bucket.endsWith(ICEBERG_BUCKET_RESERVED_SUFFIX)

      if (!isIcebergBucket) return

      let table: TableIndex | undefined

      try {
        if (icebergBucketDetectionMode === 'BUCKET') {
          table = await req.icebergCatalog?.findTableByLocation({
            location: `s3://${params.Bucket}`,
            tenantId: req.tenantId,
          })
        }

        if (icebergBucketDetectionMode === 'FULL_PATH') {
          let path = `${params.Bucket}`
          const requestedKey = params['*']

          if (requestedKey) {
            const parts = requestedKey.split('/').slice(0, 2)
            path += '/' + parts.join('/')
          }

          table = await req.icebergCatalog?.findTableByLocation({
            location: `s3://${path}`,
            tenantId: req.tenantId,
          })
        }

        if (!table) {
          return
        }

        const internalLocation = table.location.replace('s3://', '')

        if (!internalLocation) {
          return
        }

        const internalBucketName = internalLocation.split('/').shift()
        if (!internalBucketName) {
          return
        }

        req.isIcebergBucket = true
        req.internalIcebergBucketName = internalBucketName
        req.internalIcebergNamespaceId = table.namespace_id
      } catch {
        return
      }
    })
  },
  { name: 'iceberg-bucket' }
)
