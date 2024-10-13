import { FastifyBaseLogger, FastifyInstance } from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import * as http from 'http'
import { ServerOptions, DataStore } from '@tus/server'
import { getFileSizeLimit } from '@storage/limits'
import { Storage } from '@storage/storage'
import { jwt, storage, db, dbSuperUser } from '../../plugins'
import { getConfig } from '../../../config'
import {
  TusServer,
  FileStore,
  LockNotifier,
  PgLocker,
  UploadId,
  AlsMemoryKV,
} from '@storage/protocols/tus'
import {
  namingFunction,
  onCreate,
  onResponseError,
  onIncomingRequest,
  onUploadFinish,
  generateUrl,
  getFileIdFromRequest,
  SIGNED_URL_SUFFIX,
} from './lifecycle'
import { TenantConnection, PubSub } from '@internal/database'
import { S3Store } from '@tus/s3-store'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import { ROUTE_OPERATIONS } from '../operations'
import * as https from 'node:https'
import { createAgent } from '@internal/http'

const {
  storageS3MaxSockets,
  storageS3Bucket,
  storageS3Endpoint,
  storageS3ForcePathStyle,
  storageS3Region,
  storageS3ClientTimeout,
  tusUrlExpiryMs,
  tusPath,
  tusPartSize,
  tusMaxConcurrentUploads,
  uploadFileSizeLimit,
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
    isUpsert: boolean
    resources?: string[]
  }
}

function createTusStore(agent: { httpsAgent: https.Agent; httpAgent: http.Agent }) {
  if (storageBackendType === 's3') {
    return new S3Store({
      partSize: tusPartSize * 1024 * 1024, // Each uploaded part will have ${tusPartSize}MB,
      expirationPeriodInMilliseconds: tusUrlExpiryMs,
      cache: new AlsMemoryKV(),
      maxConcurrentPartUploads: tusMaxConcurrentUploads,
      s3ClientConfig: {
        requestHandler: new NodeHttpHandler({
          ...agent,
          connectionTimeout: 5000,
          requestTimeout: storageS3ClientTimeout,
        }),
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

function createTusServer(
  lockNotifier: LockNotifier,
  agent: { httpsAgent: https.Agent; httpAgent: http.Agent }
) {
  const datastore = createTusStore(agent)
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
    allowedHeaders: ['Authorization', 'X-Upsert', 'Upload-Expires', 'ApiKey', 'x-signature'],
    maxSize: async (rawReq, uploadId) => {
      const req = rawReq as MultiPartRequest

      if (!req.upload.tenantId) {
        return uploadFileSizeLimit
      }

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
  return new TusServer(serverOptions)
}

export default async function routes(fastify: FastifyInstance) {
  const lockNotifier = new LockNotifier(PubSub)
  await lockNotifier.subscribe()

  const agent = createAgent('s3_tus', {
    maxSockets: storageS3MaxSockets,
  })
  agent.monitor()

  fastify.addHook('onClose', () => {
    agent.close()
  })

  const tusServer = createTusServer(lockNotifier, agent)

  // authenticated routes
  fastify.register(async (fastify) => {
    fastify.register(jwt)
    fastify.register(db)
    fastify.register(storage)

    fastify.register(authenticatedRoutes, {
      tusServer,
    })
  })

  // signed routes
  fastify.register(
    async (fastify) => {
      fastify.register(dbSuperUser)
      fastify.register(storage)

      fastify.register(authenticatedRoutes, {
        tusServer,
        operation: '_signed',
      })
    },
    { prefix: SIGNED_URL_SUFFIX }
  )

  // public routes
  fastify.register(async (fastify) => {
    fastify.register(publicRoutes, {
      tusServer,
    })
  })

  // public signed routes
  fastify.register(
    async (fastify) => {
      fastify.register(dbSuperUser)
      fastify.register(storage)

      fastify.register(publicRoutes, {
        tusServer,
        operation: '_signed',
      })
    },
    { prefix: SIGNED_URL_SUFFIX }
  )
}

const authenticatedRoutes = fastifyPlugin(
  async (fastify: FastifyInstance, options: { tusServer: TusServer; operation?: string }) => {
    fastify.register(async function authorizationContext(fastify) {
      fastify.addContentTypeParser('application/offset+octet-stream', (request, payload, done) =>
        done(null)
      )

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
          isUpsert: req.headers['x-upsert'] === 'true',
        }
      })

      fastify.post(
        '/',
        {
          schema: { summary: 'Handle POST request for TUS Resumable uploads', tags: ['resumable'] },
          config: {
            operation: { type: `${ROUTE_OPERATIONS.TUS_CREATE_UPLOAD}${options.operation || ''}` },
          },
        },
        (req, res) => {
          options.tusServer.handle(req.raw, res.raw)
        }
      )

      fastify.post(
        '/*',
        {
          schema: { summary: 'Handle POST request for TUS Resumable uploads', tags: ['resumable'] },
          config: {
            operation: { type: `${ROUTE_OPERATIONS.TUS_CREATE_UPLOAD}${options.operation || ''}` },
          },
        },
        (req, res) => {
          options.tusServer.handle(req.raw, res.raw)
        }
      )

      fastify.put(
        '/*',
        {
          schema: { summary: 'Handle PUT request for TUS Resumable uploads', tags: ['resumable'] },
          config: {
            operation: { type: `${ROUTE_OPERATIONS.TUS_UPLOAD_PART}${options.operation || ''}` },
          },
        },
        (req, res) => {
          options.tusServer.handle(req.raw, res.raw)
        }
      )
      fastify.patch(
        '/*',
        {
          schema: {
            summary: 'Handle PATCH request for TUS Resumable uploads',
            tags: ['resumable'],
          },
          config: {
            operation: { type: `${ROUTE_OPERATIONS.TUS_UPLOAD_PART}${options.operation || ''}` },
          },
        },
        (req, res) => {
          options.tusServer.handle(req.raw, res.raw)
        }
      )
      fastify.head(
        '/*',
        {
          schema: { summary: 'Handle HEAD request for TUS Resumable uploads', tags: ['resumable'] },
          config: {
            operation: { type: `${ROUTE_OPERATIONS.TUS_GET_UPLOAD}${options.operation || ''}` },
          },
        },
        (req, res) => {
          options.tusServer.handle(req.raw, res.raw)
        }
      )
      fastify.delete(
        '/*',
        {
          schema: {
            summary: 'Handle DELETE request for TUS Resumable uploads',
            tags: ['resumable'],
          },
          config: {
            operation: { type: `${ROUTE_OPERATIONS.TUS_DELETE_UPLOAD}${options.operation || ''}` },
          },
        },
        (req, res) => {
          options.tusServer.handle(req.raw, res.raw)
        }
      )
    })
  }
)

const publicRoutes = fastifyPlugin(
  async (fastify: FastifyInstance, options: { tusServer: TusServer; operation?: string }) => {
    fastify.register(async (fastify) => {
      fastify.addContentTypeParser('application/offset+octet-stream', (request, payload, done) =>
        done(null)
      )

      fastify.addHook('preHandler', async (req) => {
        ;(req.raw as MultiPartRequest).log = req.log
        ;(req.raw as MultiPartRequest).upload = {
          storage: req.storage,
          owner: req.owner,
          tenantId: req.tenantId,
          db: req.db,
          isUpsert: req.headers['x-upsert'] === 'true',
        }
      })

      fastify.options(
        '/',
        {
          schema: {
            tags: ['resumable'],
            summary: 'Handle OPTIONS request for TUS Resumable uploads',
            description: 'Handle OPTIONS request for TUS Resumable uploads',
          },
          config: {
            operation: { type: `${ROUTE_OPERATIONS.TUS_OPTIONS}${options.operation || ''}` },
          },
        },
        (req, res) => {
          options.tusServer.handle(req.raw, res.raw)
        }
      )

      fastify.options(
        '/*',
        {
          schema: {
            tags: ['resumable'],
            summary: 'Handle OPTIONS request for TUS Resumable uploads',
            description: 'Handle OPTIONS request for TUS Resumable uploads',
          },
          config: {
            operation: { type: `${ROUTE_OPERATIONS.TUS_OPTIONS}${options.operation || ''}` },
          },
        },
        (req, res) => {
          options.tusServer.handle(req.raw, res.raw)
        }
      )
    })
  }
)
