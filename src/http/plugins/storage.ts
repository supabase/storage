import fastifyPlugin from 'fastify-plugin'
import { createDefaultDisk, createDisk, StorageDisk } from '@storage/disks'
import { getConfig } from '../../config'
import { FastifyRequest } from 'fastify'
import { Storage } from '@storage/storage'
import { createAgent } from '@internal/http'

declare module 'fastify' {
  interface FastifyRequest {
    storage: Storage
    disk: StorageDisk
  }

  interface FastifyContextConfig {
    getBucketId?: (req: FastifyRequest<any>) => Promise<string>
  }
}

const { isMultitenant, storageBackendType, storageS3MaxSockets } = getConfig()

let cachedDisk: StorageDisk | undefined

const managedS3BucketHttpAgent = createAgent('s3_default', {
  maxSockets: storageS3MaxSockets,
})
const externalS3BucketHttpAgent = createAgent('s3_external_agent', {
  maxSockets: 100,
})

export const disk = fastifyPlugin(async function diskPlugin(fastify) {
  fastify.decorateRequest('disk', null)
  fastify.addHook('preHandler', async (request) => {
    const database = request.db

    const bucketId = await getBucketIdFromRequest(request)

    if (bucketId) {
      const bucket = await database.asSuperUser().findBucketById(bucketId, 'buckets.id', {
        withDisk: true,
      })
      const credentials = bucket.credentials

      if (credentials) {
        request.disk = createDisk(storageBackendType, {
          httpAgent: externalS3BucketHttpAgent,
          bucket: bucket.mount_point,
          accessKey: credentials.access_key,
          secretKey: credentials.secret_key,
          endpoint: credentials.endpoint,
          forcePathStyle: credentials.force_path_style,
          region: credentials.region,
        })
        return
      }
    }

    if (isMultitenant) {
      request.disk = createDefaultDisk({
        prefix: request.tenantId,
        httpAgent: managedS3BucketHttpAgent,
      })
    } else {
      if (!cachedDisk) {
        cachedDisk = createDefaultDisk({
          prefix: request.tenantId,
          httpAgent: managedS3BucketHttpAgent,
        })
      }
      request.disk = cachedDisk
    }
  })
})

export const storage = fastifyPlugin(async function storagePlugin(fastify) {
  fastify.register(disk)

  fastify.decorateRequest('storage', null)
  fastify.addHook('preHandler', async (request) => {
    request.storage = new Storage(request.disk, request.db)
  })
})

function getBucketIdFromRequest(req: FastifyRequest) {
  const params = (req.params as Record<string, any>) || {}

  if ('Bucket' in params) {
    return params.Bucket as string
  }

  return req.routeConfig.getBucketId?.(req)
}
