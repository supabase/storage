import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { IncomingMessage, Server, ServerResponse } from 'http'
import { AuthenticatedRangeRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const getObjectParamsSchema = {
  type: 'object',
  properties: {
    Bucket: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['Bucket', '*'],
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
  const { Bucket } = request.params
  const { download } = request.query
  const objectName = request.params['*']

  const obj = await request.storage.from(Bucket).findObject(objectName, 'id, version')

  return request.storage.renderer('asset').render(request, response, {
    bucket: Bucket,
    key: objectName,
    version: obj.version,
    download,
  })
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object'
  fastify.get<getObjectRequestInterface>(
    '/authenticated/:Bucket/*',
    {
      exposeHeadRoute: false,
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
      config: {
        operation: { type: ROUTE_OPERATIONS.GET_AUTH_OBJECT },
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )

  fastify.get<getObjectRequestInterface>(
    '/:Bucket/*',
    {
      exposeHeadRoute: false,
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary: 'Get object',
        description: 'use GET /object/authenticated/{Bucket} instead',
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['deprecated'],
      },
      config: {
        operation: { type: ROUTE_OPERATIONS.GET_AUTH_OBJECT },
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )
}
