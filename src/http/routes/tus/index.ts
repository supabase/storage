import * as https from 'node:https'
import { S3Client } from '@aws-sdk/client-s3'
import { PubSub } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { createAgent } from '@internal/http'
import { logSchema } from '@internal/monitoring'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import { getFileSizeLimit } from '@storage/limits'
import {
  AlsMemoryKV,
  FileStore,
  LockNotifier,
  PgLocker,
  S3Store,
  UploadId,
} from '@storage/protocols/tus'
import { S3Locker } from '@storage/protocols/tus/s3-locker'
import { DataStore, Server, ServerOptions } from '@tus/server'
import type {
  FastifyInstance,
  FastifyReply,
  FastifyRequest,
  HookHandlerDoneFunction,
} from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import * as http from 'http'
import type { ServerRequest as Request } from 'srvx'
import { getConfig } from '../../../config'
import { db, dbSuperUser, registerJwtAuth, storage } from '../../plugins'
import { closeConnectionAfterResponse } from '../../plugins/close-connection'
import { ROUTE_OPERATIONS } from '../operations'
import {
  generateUrl,
  getFileIdFromRequest,
  type MultiPartRequest,
  namingFunction,
  onCreate,
  onIncomingRequest,
  onResponseError,
  onUploadFinish,
  SIGNED_URL_SUFFIX,
  verifySignedUploadRequest,
} from './lifecycle'

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
  tusAllowS3Tags,
  tusLockType,
  tusBodyIdleTimeoutMs,
  uploadFileSizeLimit,
  storageBackendType,
  storageFilePath,
} = getConfig()

function createTusStore(agent: { httpsAgent: https.Agent; httpAgent: http.Agent }) {
  if (storageBackendType === 's3') {
    return new S3Store({
      partSize: tusPartSize * 1024 * 1024, // Each uploaded part will have ${tusPartSize}MB,
      expirationPeriodInMilliseconds: tusUrlExpiryMs,
      cache: new AlsMemoryKV(),
      maxConcurrentPartUploads: tusMaxConcurrentUploads,
      useTags: tusAllowS3Tags,
      s3ClientConfig: {
        requestHandler: new NodeHttpHandler({
          ...agent,
          connectionTimeout: 5000,
          socketTimeout: storageS3ClientTimeout,
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

export function createTusLockS3Client(agent: { httpsAgent: https.Agent; httpAgent: http.Agent }) {
  const client = new S3Client({
    requestHandler: new NodeHttpHandler({
      ...agent,
      connectionTimeout: 5000,
      socketTimeout: storageS3ClientTimeout,
    }),
    region: storageS3Region,
    endpoint: storageS3Endpoint,
    forcePathStyle: storageS3ForcePathStyle,
  })
  client.middlewareStack.remove('loggerMiddleware')
  return client
}

function createTusServer(
  lockNotifier: LockNotifier,
  agent: { httpsAgent: https.Agent; httpAgent: http.Agent }
) {
  const datastore = createTusStore(agent)
  const sharedS3Client = tusLockType === 's3' ? createTusLockS3Client(agent) : undefined
  const serverOptions: ServerOptions & {
    datastore: DataStore
  } = {
    path: tusPath,
    datastore,
    disableTerminationForFinishedUploads: true,
    locker: (rawReq: Request) => {
      const req = rawReq.runtime?.node?.req as MultiPartRequest

      if (!req) {
        throw ERRORS.InternalError(undefined, 'Request object is missing')
      }

      switch (tusLockType) {
        case 'postgres':
          return new PgLocker(req.upload.storage.db, lockNotifier)

        case 's3':
          return new S3Locker({
            bucket: storageS3Bucket,
            keyPrefix: `__tus_locks/${req.upload.tenantId}/`,
            logger: console,
            lockTtlMs: 15 * 1000, // 15 seconds
            maxRetries: 10,
            retryDelayMs: 250,
            renewalIntervalMs: 10 * 1000, // 10 seconds
            s3Client: sharedS3Client!,
            notifier: lockNotifier,
          })

        default:
          throw ERRORS.InternalError(undefined, 'Unsupported TUS locker type')
      }
    },
    namingFunction,
    onUploadCreate: onCreate,
    onUploadFinish,
    onIncomingRequest: (req, id) => onIncomingRequest(req, id, datastore),
    generateUrl,
    getFileIdFromRequest,
    onResponseError,
    respectForwardedHeaders: true,
    allowedHeaders: ['Authorization', 'X-Upsert', 'Upload-Expires', 'ApiKey', 'x-signature'],
    maxSize: async (rawReq, uploadId) => {
      const req = rawReq.runtime?.node?.req as MultiPartRequest

      if (!req.upload.tenantId) {
        return uploadFileSizeLimit
      }

      if (!uploadId) {
        return getFileSizeLimit(req.upload.tenantId)
      }

      const resourceId = UploadId.fromString(uploadId)
      await verifySignedUploadRequest(req, resourceId)

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
  await lockNotifier.start()

  const agent = createAgent('s3_tus', {
    maxSockets: storageS3MaxSockets,
  })
  agent.monitor()

  fastify.addHook('onClose', async () => {
    agent.close()

    await lockNotifier.stop().catch((e) => {
      logSchema.error(fastify.log, 'Failed to stop TUS lock notifier', {
        type: 'tus',
        error: e,
      })
    })
  })

  const tusServer = createTusServer(lockNotifier, agent)

  // authenticated routes
  fastify.register(async (fastify) => {
    registerJwtAuth(fastify)
    fastify.register(db)
    fastify.register(storage)

    fastify.register(authenticatedRoutes, {
      tusServer,
      signed: false,
    })
  })

  // signed routes
  fastify.register(
    async (fastify) => {
      fastify.register(dbSuperUser)
      fastify.register(storage)

      fastify.register(authenticatedRoutes, {
        tusServer,
        signed: true,
      })
    },
    { prefix: SIGNED_URL_SUFFIX }
  )

  // public routes
  fastify.register(async (fastify) => {
    fastify.register(publicRoutes, {
      tusServer,
      signed: false,
    })
  })

  // public signed routes
  fastify.register(
    async (fastify) => {
      fastify.register(dbSuperUser)
      fastify.register(storage)

      fastify.register(publicRoutes, {
        tusServer,
        signed: true,
      })
    },
    { prefix: SIGNED_URL_SUFFIX }
  )
}

function setTusRequestContext(
  req: FastifyRequest,
  reply: FastifyReply,
  done: HookHandlerDoneFunction,
  isSigned: boolean
) {
  // TUS protocol rejections write directly and skip Fastify's onSend hook.
  const writeHead = reply.raw.writeHead
  reply.raw.writeHead = (...args) => {
    if (args[0] >= 400) {
      closeConnectionAfterResponse(req.raw, reply.raw, req.raw.executionError)
    }
    return Reflect.apply(writeHead, reply.raw, args)
  }
  reply.raw.once('close', () => req.db?.dispose())

  ;(req.raw as MultiPartRequest).log = req.log
  ;(req.raw as MultiPartRequest).upload = {
    tenantId: req.tenantId,
    storage: req.storage,
    owner: req.owner,
    db: req.db,
    isUpsert: req.headers['x-upsert'] === 'true',
    isSigned,
    reqId: req.id,
    sbReqId: req.sbReqId,
  }
  done()
}

export async function handleTusRequestWithIdleTimeout(
  tusServer: Server,
  req: FastifyRequest,
  res: FastifyReply
) {
  const socket = req.raw.socket
  const isDone = () => req.raw.complete || req.raw.readableEnded || req.raw.destroyed
  const hasDeclaredBody =
    req.raw.headers['transfer-encoding'] !== undefined ||
    Number(req.raw.headers['content-length']) > 0
  if (!socket || tusBodyIdleTimeoutMs <= 0 || !hasDeclaredBody || isDone()) {
    return tusServer.handle(req.raw, res.raw)
  }

  // We use our own timer instead of socket or stream events, so we don't interfere with how the body gets read
  // bytesRead alone is not enough, because it also stalls when our own write to disk or S3 is slow
  // bytesConsumed tracks the write side, so we can tell those two cases apart
  // If either one is moving, the upload is alive
  let lastBytesRead = socket.bytesRead
  let lastBytesConsumed = lastBytesRead - req.raw.readableLength
  let idleTimer: NodeJS.Timeout

  const disarm = () => {
    clearTimeout(idleTimer)
    req.raw.removeListener('end', disarm)
    req.raw.removeListener('close', disarm)
  }

  const checkIdle = () => {
    if (isDone()) {
      disarm()
      return
    }

    const currentBytesRead = socket.bytesRead
    const currentBytesConsumed = currentBytesRead - req.raw.readableLength
    if (currentBytesRead > lastBytesRead || currentBytesConsumed > lastBytesConsumed) {
      lastBytesRead = currentBytesRead
      lastBytesConsumed = currentBytesConsumed
      idleTimer = setTimeout(checkIdle, tusBodyIdleTimeoutMs)
      return
    }

    const err = ERRORS.TusError('TUS request body idle timeout - no bytes received', 408)
    req.raw.executionError = err
    req.raw.destroy(err)
  }

  idleTimer = setTimeout(checkIdle, tusBodyIdleTimeoutMs)
  // Stop tracking idle time once the client has sent the full body, so
  // slow lock acquisition or upload finalization afterward can't trip a
  // "no bytes received" timeout
  req.raw.once('end', disarm)
  // A disconnect never fires 'end', so also disarm on 'close'
  req.raw.once('close', disarm)

  try {
    return await tusServer.handle(req.raw, res.raw)
  } finally {
    disarm()
  }
}

export const authenticatedRoutes = fastifyPlugin(
  async (fastify: FastifyInstance, options: { tusServer: Server; signed: boolean }) => {
    const operationSuffix = options.signed ? '_signed' : ''
    fastify.register(async function authorizationContext(fastify) {
      fastify.addContentTypeParser('application/offset+octet-stream', (request, payload, done) =>
        done(null)
      )

      fastify.addHook('onRequest', (req, res, done) => {
        AlsMemoryKV.localStorage.run(new Map(), () => {
          done()
        })
      })

      fastify.addHook('preHandler', (req, res, done) =>
        setTusRequestContext(req, res, done, options.signed)
      )

      fastify.post(
        '/',
        {
          schema: { summary: 'Handle POST request for TUS Resumable uploads', tags: ['resumable'] },
          config: {
            operation: `${ROUTE_OPERATIONS.TUS_CREATE_UPLOAD}${operationSuffix}`,
          },
        },
        async (req, res) => {
          await handleTusRequestWithIdleTimeout(options.tusServer, req, res)
        }
      )

      fastify.post(
        '/*',
        {
          schema: { summary: 'Handle POST request for TUS Resumable uploads', tags: ['resumable'] },
          config: {
            operation: `${ROUTE_OPERATIONS.TUS_CREATE_UPLOAD}${operationSuffix}`,
          },
        },
        async (req, res) => {
          await handleTusRequestWithIdleTimeout(options.tusServer, req, res)
        }
      )

      fastify.put(
        '/*',
        {
          schema: { summary: 'Handle PUT request for TUS Resumable uploads', tags: ['resumable'] },
          config: {
            operation: `${ROUTE_OPERATIONS.TUS_UPLOAD_PART}${operationSuffix}`,
          },
        },
        async (req, res) => {
          await handleTusRequestWithIdleTimeout(options.tusServer, req, res)
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
            operation: `${ROUTE_OPERATIONS.TUS_UPLOAD_PART}${operationSuffix}`,
          },
        },
        async (req, res) => {
          await handleTusRequestWithIdleTimeout(options.tusServer, req, res)
        }
      )
      fastify.head(
        '/*',
        {
          schema: { summary: 'Handle HEAD request for TUS Resumable uploads', tags: ['resumable'] },
          config: {
            operation: `${ROUTE_OPERATIONS.TUS_GET_UPLOAD}${operationSuffix}`,
          },
        },
        async (req, res) => {
          await options.tusServer.handle(req.raw, res.raw)
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
            operation: `${ROUTE_OPERATIONS.TUS_DELETE_UPLOAD}${operationSuffix}`,
          },
        },
        async (req, res) => {
          await options.tusServer.handle(req.raw, res.raw)
        }
      )
    })
  }
)

export const publicRoutes = fastifyPlugin(
  async (fastify: FastifyInstance, options: { tusServer: Server; signed: boolean }) => {
    const operationSuffix = options.signed ? '_signed' : ''
    fastify.register(async (fastify) => {
      fastify.addContentTypeParser('application/offset+octet-stream', (request, payload, done) =>
        done(null)
      )

      fastify.addHook('preHandler', (req, res, done) =>
        setTusRequestContext(req, res, done, options.signed)
      )

      fastify.options(
        '/',
        {
          schema: {
            tags: ['resumable'],
            summary: 'Handle OPTIONS request for TUS Resumable uploads',
            description: 'Handle OPTIONS request for TUS Resumable uploads',
          },
          config: {
            operation: `${ROUTE_OPERATIONS.TUS_OPTIONS}${operationSuffix}`,
          },
        },
        async (req, res) => {
          await options.tusServer.handle(req.raw, res.raw)
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
            operation: `${ROUTE_OPERATIONS.TUS_OPTIONS}${operationSuffix}`,
          },
        },
        async (req, res) => {
          await options.tusServer.handle(req.raw, res.raw)
        }
      )
    })
  }
)
