import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'
import { getServiceKeyUser } from '../../database'
import { SignatureV4 } from '../../storage/protocols/s3'
import { ERRORS } from '../../storage'

const { storageS3Region, s3ProtocolPrefix, s3ProtocolEnforceRegion } = getConfig()

export const signatureV4 = fastifyPlugin(async function (fastify: FastifyInstance) {
  fastify.addHook('preHandler', async (request: FastifyRequest, reply: FastifyReply) => {
    const awsRegion = storageS3Region
    const awsService = 's3'

    const serviceKey = await getServiceKeyUser(request.tenantId)
    const signatureV4 = new SignatureV4({
      region: awsRegion,
      service: awsService,
      tenantId: request.tenantId,
      secretKey: serviceKey.jwt,
      enforceRegion: s3ProtocolEnforceRegion,
    })

    try {
      const isVerified = signatureV4.verify({
        url: request.url,
        body: request.body as string | ReadableStream | Buffer,
        headers: request.headers as Record<string, string | string[]>,
        method: request.method,
        query: request.query as Record<string, string>,
        prefix: s3ProtocolPrefix,
      })

      if (!isVerified) {
        throw ERRORS.AccessDenied('Invalid Signature')
      }

      request.jwt = serviceKey.jwt
    } catch (e) {
      throw e
    }
  })
})
