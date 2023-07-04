import { FastifyInstance } from 'fastify'
import { Metadata, Server } from '@tus/server'
import { jwt, storage, db, dbSuperUser, parentBucket } from '../../plugins'
import { getConfig } from '../../../config'
import * as http from 'http'
import { Storage, StorageBackendError } from '../../../storage'
import { S3Store } from './s3-store'
import { Head, Options, Patch, Post } from './handlers'
import { namingFunction, onCreate, onUploadFinish } from './lifecycle'
import { ServerOptions } from '@tus/server/types'
import { DataStore } from '@tus/server/models'
import { getFileSizeLimit } from '../../../storage/limits'
import { FileStore } from './file-store'
import { BucketWithCredentials } from '../../../storage/schemas'
import { decrypt } from '../../../auth'
import { getFileIdFromRequest, UploadId } from './upload-id'
import { TenantConnection } from '../../../database/connection'

const {
  globalS3Bucket,
  globalS3Endpoint,
  globalS3Protocol,
  globalS3ForcePathStyle,
  region,
  tusUrlExpiryMs,
  tusPath,
  storageBackendType,
  fileStoragePath,
} = getConfig()

type MultiPartRequest = http.IncomingMessage & {
  upload: {
    storage: Storage
    owner?: string
    tenantId: string
    db: TenantConnection
    bucket: BucketWithCredentials
  }
}

const defaultTusStore = new S3Store({
  partSize: 6 * 1024 * 1024, // Each uploaded part will have ~6MB,
  uploadExpiryMilliseconds: tusUrlExpiryMs,
  s3ClientConfig: {
    bucket: globalS3Bucket,
    region: region,
    endpoint: globalS3Endpoint,
    sslEnabled: globalS3Protocol !== 'http',
    s3ForcePathStyle: globalS3ForcePathStyle,
  },
})

function createTusStore(bucket: BucketWithCredentials) {
  if (bucket.credential_id) {
    return new S3Store({
      partSize: 6 * 1024 * 1024, // Each uploaded part will have ~6MB,
      uploadExpiryMilliseconds: tusUrlExpiryMs,
      s3ClientConfig: {
        bucket: bucket.id,
        endpoint: bucket.endpoint,
        region: bucket.region,
        s3ForcePathStyle: Boolean(bucket.force_path_style),
        credentials:
          bucket.access_key && bucket.secret_key
            ? {
                accessKeyId: decrypt(bucket.access_key),
                secretAccessKey: decrypt(bucket.secret_key),
              }
            : undefined,
      },
    })
  }

  if (storageBackendType === 's3') {
    return defaultTusStore
  }

  return new FileStore({
    directory: fileStoragePath + '/' + globalS3Bucket,
  })
}

function createTusServer(datastore: DataStore) {
  const serverOptions: ServerOptions & {
    datastore: DataStore
  } = {
    path: tusPath,
    datastore: datastore,
    namingFunction: namingFunction,
    onUploadCreate: onCreate,
    onUploadFinish: onUploadFinish,
    respectForwardedHeaders: true,
    maxFileSize: async (id, rawReq) => {
      const req = rawReq as MultiPartRequest

      const bucket = await req.upload.bucket

      const globalFileLimit = await getFileSizeLimit(req.upload.tenantId)

      const fileSizeLimit = bucket.file_size_limit || globalFileLimit
      if (fileSizeLimit > globalFileLimit) {
        return globalFileLimit
      }

      return fileSizeLimit
    },
  }
  const tusServer = new Server(serverOptions)

  tusServer.handlers.PATCH = new Patch(datastore, serverOptions)
  tusServer.handlers.HEAD = new Head(datastore, serverOptions)
  tusServer.handlers.POST = new Post(datastore, serverOptions)
  tusServer.handlers.OPTIONS = new Options(datastore, serverOptions)

  return tusServer
}

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async function authorizationContext(fastify) {
    fastify.addContentTypeParser('application/offset+octet-stream', (request, payload, done) =>
      done(null)
    )

    fastify.addHook('preHandler', async (req) => {
      ;(req.raw as any).upload = {
        tenantId: req.tenantId,
      }
    })

    fastify.register(jwt)
    fastify.register(db)
    fastify.register(parentBucket)
    fastify.register(storage)

    fastify.addHook('preHandler', async (req) => {
      ;(req.raw as MultiPartRequest).upload = {
        storage: req.storage,
        owner: req.owner,
        tenantId: req.tenantId,
        db: req.dbConnection,
        bucket: req.bucket,
      }
    })

    fastify.post(
      '/',
      {
        schema: { summary: 'Handle POST request for TUS Resumable uploads', tags: ['object'] },
        config: {
          getParentBucketId: (req) => {
            const metadataHeader = req.headers['upload-metadata']

            if (typeof metadataHeader !== 'string') {
              throw new StorageBackendError('invalid_metadata', 400, 'invalid metadata')
            }

            const params = Metadata.parse(metadataHeader)
            return params.bucketName || ''
          },
        },
      },
      (req, res) => {
        const datastore = createTusStore(req.bucket)
        const tusServer = createTusServer(datastore)

        tusServer.handle(req.raw, res.raw)
      }
    )

    fastify.post(
      '/*',
      {
        schema: { summary: 'Handle POST request for TUS Resumable uploads', tags: ['object'] },
        config: {
          getParentBucketId: (req) => {
            const metadataHeader = req.headers['upload-metadata']

            if (typeof metadataHeader !== 'string') {
              throw new StorageBackendError('invalid_metadata', 400, 'invalid metadata')
            }

            const params = Metadata.parse(metadataHeader)
            return params.bucketName || ''
          },
        },
      },
      (req, res) => {
        const datastore = createTusStore(req.bucket)
        const tusServer = createTusServer(datastore)

        tusServer.handle(req.raw, res.raw)
      }
    )

    fastify.put(
      '/*',
      { schema: { summary: 'Handle PUT request for TUS Resumable uploads', tags: ['object'] } },
      (req, res) => {
        const datastore = createTusStore(req.bucket)
        const tusServer = createTusServer(datastore)
        tusServer.handle(req.raw, res.raw)
      }
    )
    fastify.patch(
      '/*',
      {
        schema: { summary: 'Handle PATCH request for TUS Resumable uploads', tags: ['object'] },
        config: {
          getParentBucketId: (req) => {
            const id = getFileIdFromRequest(req.raw, tusPath)

            if (!id) {
              throw new StorageBackendError('invalid_id', 400, 'invalid id')
            }
            const uploadId = UploadId.fromString(id)
            return uploadId.bucket
          },
        },
      },
      (req, res) => {
        const datastore = createTusStore(req.bucket)
        const tusServer = createTusServer(datastore)
        tusServer.handle(req.raw, res.raw)
      }
    )
    fastify.head(
      '/*',
      {
        schema: { summary: 'Handle HEAD request for TUS Resumable uploads', tags: ['object'] },
        config: {
          getParentBucketId: (req) => {
            const id = getFileIdFromRequest(req.raw, tusPath)

            if (!id) {
              throw new StorageBackendError('invalid_id', 400, 'invalid id')
            }
            const uploadId = UploadId.fromString(id)
            return uploadId.bucket
          },
        },
      },
      (req, res) => {
        const datastore = createTusStore(req.bucket)
        const tusServer = createTusServer(datastore)
        tusServer.handle(req.raw, res.raw)
      }
    )
  })

  fastify.register(async function authorizationContext(fastify) {
    fastify.addContentTypeParser('application/offset+octet-stream', (request, payload, done) =>
      done(null)
    )

    fastify.addHook('preHandler', async (req) => {
      ;(req.raw as any).upload = {
        tenantId: req.tenantId,
      }
    })

    fastify.register(dbSuperUser)
    fastify.register(parentBucket)
    fastify.register(storage)

    fastify.addHook('preHandler', async (req) => {
      ;(req.raw as Omit<MultiPartRequest, 'bucket'>).upload = {
        storage: req.storage,
        owner: req.owner,
        tenantId: req.tenantId,
        db: req.dbConnection,
        bucket: req.bucket,
      }
    })

    fastify.options(
      '/',
      {
        schema: {
          tags: ['object'],
          summary: 'Handle OPTIONS request for TUS Resumable uploads',
          description: 'Handle OPTIONS request for TUS Resumable uploads',
        },
        config: {
          getParentBucketId: false,
        },
      },
      (req, res) => {
        const datastore = createTusStore({} as BucketWithCredentials)
        const tusServer = createTusServer(datastore)
        tusServer.handle(req.raw, res.raw)
      }
    )

    fastify.options(
      '/*',
      {
        schema: {
          tags: ['object'],
          summary: 'Handle OPTIONS request for TUS Resumable uploads',
          description: 'Handle OPTIONS request for TUS Resumable uploads',
        },
        config: {
          getParentBucketId: (req) => {
            const id = getFileIdFromRequest(req.raw, tusPath)

            if (!id) {
              throw new StorageBackendError('invalid_id', 400, 'invalid id')
            }
            const uploadId = UploadId.fromString(id)
            return uploadId.bucket
          },
        },
      },
      (req, res) => {
        const datastore = createTusStore(req.bucket)
        const tusServer = createTusServer(datastore)
        tusServer.handle(req.raw, res.raw)
      }
    )
  })
}
