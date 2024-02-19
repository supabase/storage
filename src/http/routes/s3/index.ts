import { FastifyInstance } from 'fastify'
import { db, jsonToXml, signatureV4, storage } from '../../plugins'
import { S3ProtocolHandler } from '../../../storage/protocols/s3/handler'
import { Router } from '../../../storage/protocols/s3/router'

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async (fastify) => {
    fastify.register(signatureV4)
    fastify.register(jsonToXml)
    fastify.register(db)
    fastify.register(storage)

    const s3Router = new Router()

    const s3Routes = s3Router.routes()
    Object.keys(s3Routes).forEach((route) => {
      fastify.all(route, async (req, reply) => {
        const routeMatch = s3Routes[route as keyof typeof s3Routes]

        const routeHandler = routeMatch(req as any)

        if (!routeHandler) {
          return reply.status(404).send()
        }

        const s3Protocol = new S3ProtocolHandler(req.storage, req.tenantId)
        const output = await routeHandler(s3Protocol)

        const headers = output.headers

        if (headers) {
          Object.keys(headers).forEach((header) => {
            reply.header(header, headers[header])
          })
        }

        return reply.status(output.statusCode || 200).send(output.responseBody)
      })
    })
  })
}
