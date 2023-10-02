import { FastifyBaseLogger, FastifyInstance } from 'fastify'
import { Server } from '@tus/server'
import { jwt, storage, db, dbSuperUser } from '../../plugins'
import { getConfig } from '../../../config'
import * as http from 'http'
import { Storage } from '../../../storage'
import { S3Store } from './s3-store'
import { Head, Options, Patch, Post } from './handlers'
import { namingFunction, onCreate, onUploadFinish } from './lifecycle'
import { ServerOptions } from '@tus/server/types'
import { DataStore } from '@tus/server/models'
import { getFileSizeLimit } from '../../../storage/limits'
import { UploadId } from './upload-id'
import { FileStore } from './file-store'
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
  log: FastifyBaseLogger
  upload: {
    storage: Storage
    owner?: string
    tenantId: string
    db: TenantConnection
  }
}

function createTusStore() {
  if (storageBackendType === 's3') {
    return new S3Store({
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
  }

  return new FileStore({
    directory: fileStoragePath + '/' + globalS3Bucket,
  })
}

function createTusServer() {
  const datastore = createTusStore()
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

      const resourceId = UploadId.fromString(id)

      const bucket = await req.upload.storage
        .asSuperUser()
        .findBucket(resourceId.bucket, 'id,file_size_limit')

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
  const tusServer = createTusServer()

  fastify.register(async function authorizationContext(fastify) {
    fastify.addContentTypeParser('application/offset+octet-stream', (request, payload, done) =>
      done(null)
    )

    fastify.register(jwt)
    fastify.register(db)
    fastify.register(storage)

    fastify.addHook('preHandler', async (req) => {
      ;(req.raw as MultiPartRequest).log = req.log
      ;(req.raw as MultiPartRequest).upload = {
        storage: req.storage,
        owner: req.owner,
        tenantId: req.tenantId,
        db: req.db,
      }
    })

    fastify.post(
      '/',
      { schema: { summary: 'Handle POST request for TUS Resumable uploads', tags: ['object'] } },
      (req, res) => {
        tusServer.handle(req.raw, res.raw)
      }
    )

    fastify.post(
      '/*',
      { schema: { summary: 'Handle POST request for TUS Resumable uploads', tags: ['object'] } },
      (req, res) => {
        tusServer.handle(req.raw, res.raw)
      }
    )

    fastify.put(
      '/*',
      { schema: { summary: 'Handle PUT request for TUS Resumable uploads', tags: ['object'] } },
      (req, res) => {
        tusServer.handle(req.raw, res.raw)
      }
    )
    fastify.patch(
      '/*',
      { schema: { summary: 'Handle PATCH request for TUS Resumable uploads', tags: ['object'] } },
      (req, res) => {
        tusServer.handle(req.raw, res.raw)
      }
    )
    fastify.head(
      '/*',
      { schema: { summary: 'Handle HEAD request for TUS Resumable uploads', tags: ['object'] } },
      (req, res) => {
        tusServer.handle(req.raw, res.raw)
      }
    )
  })

  fastify.register(async function authorizationContext(fastify) {
    fastify.addContentTypeParser('application/offset+octet-stream', (request, payload, done) =>
      done(null)
    )

    fastify.register(dbSuperUser)
    fastify.register(storage)

    fastify.addHook('preHandler', async (req) => {
      ;(req.raw as MultiPartRequest).log = req.log
      ;(req.raw as MultiPartRequest).upload = {
        storage: req.storage,
        owner: req.owner,
        tenantId: req.tenantId,
        db: req.db,
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
      },
      (req, res) => {
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
      },
      (req, res) => {
        tusServer.handle(req.raw, res.raw)
      }
    )
  })
}
