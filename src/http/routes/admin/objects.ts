import { FastifyInstance, RequestGenericInterface } from 'fastify'
import apiKey from '../../plugins/apikey'
import { dbSuperUser, storage } from '../../plugins'
import { ObjectScanner } from '@storage/scanner/scanner'
import { FastifyReply } from 'fastify/types/reply'

const listOrphanedObjects = {
  description: 'List Orphaned Objects',
  params: {
    type: 'object',
    properties: {
      tenantId: { type: 'string' },
      bucketId: { type: 'string' },
    },
    required: ['tenantId', 'bucketId'],
  },
  query: {
    type: 'object',
    properties: {
      before: { type: 'string' },
      keepTmpTable: { type: 'boolean' },
    },
  },
} as const

const syncOrphanedObjects = {
  description: 'Sync Orphaned Objects',
  params: {
    type: 'object',
    properties: {
      tenantId: { type: 'string' },
      bucketId: { type: 'string' },
    },
    required: ['tenantId', 'bucketId'],
  },
  body: {
    type: 'object',
    properties: {
      deleteDbKeys: { type: 'boolean' },
      deleteS3Keys: { type: 'boolean' },
      tmpTable: { type: 'string' },
    },
  },
  optional: ['deleteDbKeys', 'deleteS3Keys'],
} as const

interface ListOrphanObjectsRequest extends RequestGenericInterface {
  Params: {
    tenantId: string
    bucketId: string
  }
  Querystring: {
    before?: string
    keepTmpTable?: boolean
  }
}

interface SyncOrphanObjectsRequest extends RequestGenericInterface {
  Params: {
    tenantId: string
    bucketId: string
  }
  Body: {
    deleteDbKeys?: boolean
    deleteS3Keys?: boolean
    before?: string
    tmpTable?: string
    keepTmpTable?: boolean
  }
}

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)
  fastify.register(dbSuperUser, {
    disableHostCheck: true,
    maxConnections: 5,
  })
  fastify.register(storage)

  fastify.get<ListOrphanObjectsRequest>(
    '/:tenantId/buckets/:bucketId/orphan-objects',
    {
      schema: listOrphanedObjects,
    },
    async (req, reply) => {
      const bucket = req.params.bucketId
      let before = req.query.before ? new Date(req.query.before as string) : undefined

      if (before && isNaN(before.getTime())) {
        return reply.status(400).send({
          error: 'Invalid date format',
        })
      }
      if (!before) {
        before = new Date()
        before.setHours(before.getHours() - 1)
      }

      const scanner = new ObjectScanner(req.storage)
      const orphanObjects = scanner.listOrphaned(bucket, {
        signal: req.signals.disconnect.signal,
        before: before,
        keepTmpTable: Boolean(req.query.keepTmpTable),
      })

      reply.header('Content-Type', 'application/json; charset=utf-8')

      // Do not let the connection time out, periodically send
      // a ping message to keep the connection alive
      const respPing = ping(reply)

      try {
        for await (const result of orphanObjects) {
          if (result.value.length > 0) {
            respPing.update()
            reply.raw.write(
              JSON.stringify({
                ...result,
                event: 'data',
              })
            )
          }
        }
      } catch (e) {
        throw e
      } finally {
        respPing.clear()
        reply.raw.end()
      }
    }
  )

  fastify.delete<SyncOrphanObjectsRequest>(
    '/:tenantId/buckets/:bucketId/orphan-objects',
    {
      schema: syncOrphanedObjects,
    },
    async (req, reply) => {
      if (!req.body.deleteDbKeys && !req.body.deleteS3Keys) {
        return reply.status(400).send({
          error: 'At least one of deleteDbKeys or deleteS3Keys must be set to true',
        })
      }

      const bucket = `${req.params.bucketId}`
      let before = req.body.before ? new Date(req.body.before as string) : undefined

      if (!before) {
        before = new Date()
        before.setHours(before.getHours() - 1)
      }

      const respPing = ping(reply)

      try {
        const scanner = new ObjectScanner(req.storage)
        const result = scanner.deleteOrphans(bucket, {
          deleteDbKeys: req.body.deleteDbKeys,
          deleteS3Keys: req.body.deleteS3Keys,
          signal: req.signals.disconnect.signal,
          before,
          tmpTable: req.body.tmpTable,
        })

        for await (const deleted of result) {
          respPing.update()
          reply.raw.write(
            JSON.stringify({
              ...deleted,
              event: 'data',
            })
          )
        }
      } catch (e) {
        throw e
      } finally {
        respPing.clear()
        reply.raw.end()
      }
    }
  )
}

// Occasionally write a ping message to the response stream
function ping(reply: FastifyReply) {
  let lastSend = undefined as Date | undefined
  const clearPing = setInterval(() => {
    const fiveSecondsEarly = new Date()
    fiveSecondsEarly.setSeconds(fiveSecondsEarly.getSeconds() - 5)

    if (!lastSend || (lastSend && lastSend < fiveSecondsEarly)) {
      lastSend = new Date()
      reply.raw.write(
        JSON.stringify({
          event: 'ping',
        })
      )
    }
  }, 1000 * 10)

  return {
    clear: () => clearInterval(clearPing),
    update: () => {
      lastSend = new Date()
    },
  }
}
