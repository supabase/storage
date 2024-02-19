import { FastifyBaseLogger, FastifyInstance } from 'fastify'
import * as http from 'http'
import { Server, ServerOptions, DataStore } from '@tus/server'
import { jwt, storage, db, dbSuperUser } from '../../plugins'
import { getConfig } from '../../../config'
import { getFileSizeLimit } from '../../../storage/limits'
import { Storage } from '../../../storage'
import {
  FileStore,
  LockNotifier,
  PgLocker,
  UploadId,
  AlsMemoryKV,
} from '../../../storage/protocols/tus'
import {
  namingFunction,
  onCreate,
  onResponseError,
  onIncomingRequest,
  onUploadFinish,
  generateUrl,
  getFileIdFromRequest,
} from './lifecycle'
import { TenantConnection } from '../../../database/connection'
import { PubSub } from '../../../database/pubsub'
import { S3Store } from '@tus/s3-store'

const {
  storageS3Bucket,
  storageS3Endpoint,
  storageS3ForcePathStyle,
  storageS3Region,
  tusUrlExpiryMs,
  tusPath,
  storageBackendType,
  storageFilePath,
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
      expirationPeriodInMilliseconds: tusUrlExpiryMs,
      cache: new AlsMemoryKV(),
      s3ClientConfig: {
        bucket: storageS3Bucket,
        region: storageS3Region,
        endpoint: storageS3Endpoint,
        forcePathStyle: storageS3ForcePathStyle,
      },
    })
  }

  return new FileStore({
    directory: storageFilePath + '/' + storageS3Bucket,
  })
}

function createTusServer(lockNotifier: LockNotifier) {
  const datastore = createTusStore()
  const serverOptions: ServerOptions & {
    datastore: DataStore
  } = {
    path: tusPath,
    datastore: datastore,
    disableTerminationForFinishedUploads: true,
    locker: (rawReq: http.IncomingMessage) => {
      const req = rawReq as MultiPartRequest
      return new PgLocker(req.upload.storage.db, lockNotifier)
    },
    namingFunction: namingFunction,
    onUploadCreate: onCreate,
    onUploadFinish: onUploadFinish,
    onIncomingRequest: onIncomingRequest,
    generateUrl: generateUrl,
    getFileIdFromRequest: getFileIdFromRequest,
    onResponseError: onResponseError,
    respectForwardedHeaders: true,
    allowedHeaders: ['Authorization', 'X-Upsert', 'Upload-Expires', 'ApiKey'],
    maxSize: async (rawReq, uploadId) => {
      const req = rawReq as MultiPartRequest

      if (!uploadId) {
        return getFileSizeLimit(req.upload.tenantId)
      }

      const resourceId = UploadId.fromString(uploadId)

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
  return new Server(serverOptions)
}

export default async function routes(fastify: FastifyInstance) {
  const lockNotifier = new LockNotifier(PubSub)
  await lockNotifier.subscribe()

  const tusServer = createTusServer(lockNotifier)

  fastify.register(async function authorizationContext(fastify) {
    fastify.addContentTypeParser('application/offset+octet-stream', (request, payload, done) =>
      done(null)
    )

    fastify.register(jwt)
    fastify.register(db)
    fastify.register(storage)

    fastify.addHook('onRequest', (req, res, done) => {
      AlsMemoryKV.localStorage.run(new Map(), () => {
        done()
      })
    })

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
    fastify.delete(
      '/*',
      { schema: { summary: 'Handle DELETE request for TUS Resumable uploads', tags: ['object'] } },
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
