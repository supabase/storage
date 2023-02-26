import { FastifyInstance } from 'fastify'
import { Server } from '@tus/server'
import { FileStore } from '@tus/file-store'
import { jwt, storage, db, dbSuperUser } from '../../plugins'
import { getConfig } from '../../../config'
import * as http from 'http'
import { Storage } from '../../../storage'
import { S3Store } from './s3-store'
import { Head, Patch, Post } from './handlers'
import { namingFunction, onCreate, onUploadFinish } from './lifecycle'
import { ServerOptions } from '@tus/server/dist/types'
import { DataStore } from '@tus/server/dist/models'

const { globalS3Bucket, globalS3Endpoint, region, fileStoragePath, tenantId } = getConfig()

type MultiPartRequest = http.IncomingMessage & {
  upload: {
    storage: Storage
    owner?: string
    tenantId: string
  }
}

export default async function routes(fastify: FastifyInstance) {
  const s3Store = new S3Store({
    partSize: 6 * 1024 * 1024, // Each uploaded part will have ~8MB,
    uploadExpiryMilliseconds: 1000 * 1000,
    s3ClientConfig: {
      bucket: globalS3Bucket,
      region: region,
      endpoint: globalS3Endpoint,
    },
  })

  const fileStore = new FileStore({
    directory: fileStoragePath + '/' + globalS3Bucket + '/' + tenantId,
  })

  const serverOptions: ServerOptions & {
    datastore: DataStore
  } = {
    path: '/multi-part',
    datastore: s3Store,
    namingFunction: namingFunction,
    onUploadCreate: onCreate,
    onUploadFinish: onUploadFinish,
  }
  const tusServer = new Server(serverOptions)

  tusServer.handlers.PATCH = new Patch(s3Store, serverOptions)
  tusServer.handlers.HEAD = new Head(s3Store, serverOptions)
  tusServer.handlers.POST = new Post(s3Store, serverOptions)

  fastify.register(async function authorizationContext(fastify) {
    fastify.addContentTypeParser('application/offset+octet-stream', (request, payload, done) =>
      done(null)
    )

    fastify.register(jwt)
    fastify.register(db)
    fastify.register(dbSuperUser)
    fastify.register(storage)

    fastify.addHook('preHandler', async (req) => {
      ;(req.raw as MultiPartRequest).upload = {
        storage: req.storage,
        owner: req.owner,
        tenantId: req.tenantId,
      }
    })

    fastify.post('/', (req, res) => {
      tusServer.handle(req.raw, res.raw)
    })

    fastify.post('/*', (req, res) => {
      tusServer.handle(req.raw, res.raw)
    })

    fastify.put('/*', (req, res) => {
      tusServer.handle(req.raw, res.raw)
    })
    fastify.patch('/*', (req, res) => {
      tusServer.handle(req.raw, res.raw)
    })
    fastify.head('/*', (req, res) => {
      tusServer.handle(req.raw, res.raw)
    })
  })
}
