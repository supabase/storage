import fastifyPlugin from 'fastify-plugin'
import { FastifyInstance } from 'fastify'
import { multitenantKnex } from '@internal/database'
import {
  VectorStore,
  createS3VectorClient,
  KnexVectorDB,
  VectorStoreManager,
  S3Vector,
} from '@storage/protocols/vector'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    s3Vector: VectorStore
  }
}

const { isMultitenant } = getConfig()

const s3VectorClient = createS3VectorClient()
const s3VectorAdapter = new S3Vector(s3VectorClient)

export const s3vector = fastifyPlugin(async function (fastify: FastifyInstance) {
  fastify.addHook('preHandler', async (req) => {
    const db = isMultitenant ? multitenantKnex : req.db.pool.acquire()
    const store = new KnexVectorDB(db)
    req.s3Vector = new VectorStoreManager(s3VectorAdapter, store, { tenantId: req.tenantId })
  })
})
