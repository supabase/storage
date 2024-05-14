import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { IncomingMessage, Server, ServerResponse } from 'http'
import { getConfig } from '../../../config'
import { AuthenticatedRangeRequest } from '../../request'

const { storageS3Bucket } = getConfig()

const getObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
} as const

const getObjectQuerySchema = {
  type: 'object',
  properties: {
    download: { type: 'string', examples: ['filename.jpg', null] },
  },
} as const

interface getObjectRequestInterface extends AuthenticatedRangeRequest {
  Params: FromSchema<typeof getObjectParamsSchema>
  Querystring: FromSchema<typeof getObjectQuerySchema>
}

async function requestHandler(
  request: FastifyRequest<getObjectRequestInterface, Server, IncomingMessage>,
  response: FastifyReply<
    Server,
    IncomingMessage,
    ServerResponse,
    getObjectRequestInterface,
    unknown
  >
) {
  const { bucketName } = request.params
  const { download } = request.query
  const objectName = request.params['*']

  const obj = await request.storage.from(bucketName).findObject(objectName, 'id, version')

  // send the object from s3
  const s3Key = `${request.tenantId}/${bucketName}/${objectName}`

  return request.storage.renderer('asset').render(request, response, {
    bucket: storageS3Bucket,
    key: s3Key,
    version: obj.version,
    download,
  })
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object'
  fastify.get<getObjectRequestInterface>(
    '/authenticated/:bucketName/*',
    {
      exposeHeadRoute: false,
      config: {
        allowQueryStringToken: true,
      },
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        headers: { type: 'object', properties: { authorization: { type: 'string' } } },
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )

  fastify.get<getObjectRequestInterface>(
    '/:bucketName/*',
    {
      exposeHeadRoute: false,
      config: {
        allowQueryStringToken: true,
      },
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        headers: { type: 'object', properties: { authorization: { type: 'string' } } },
        summary: 'Get object',
        description: 'use GET /object/authenticated/{bucketName} instead',
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['deprecated'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )
}
