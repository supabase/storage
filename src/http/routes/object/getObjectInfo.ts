import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { IncomingMessage, Server, ServerResponse } from 'http'
import { AuthenticatedRangeRequest } from '../../types'
import { Obj } from '@storage/schemas'
import { ROUTE_OPERATIONS } from '../operations'

const getObjectParamsSchema = {
  type: 'object',
  properties: {
    Bucket: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['Bucket', '*'],
} as const

interface getObjectRequestInterface extends AuthenticatedRangeRequest {
  Params: FromSchema<typeof getObjectParamsSchema>
}

async function requestHandler(
  request: FastifyRequest<getObjectRequestInterface, Server, IncomingMessage>,
  response: FastifyReply<
    Server,
    IncomingMessage,
    ServerResponse,
    getObjectRequestInterface,
    unknown
  >,
  publicRoute = false,
  method: 'head' | 'info' = 'head'
) {
  const { Bucket } = request.params
  const objectName = request.params['*']

  let obj: Obj
  if (publicRoute) {
    await request.storage.asSuperUser().findBucket(Bucket, 'id', {
      isPublic: true,
    })
    obj = await request.storage
      .asSuperUser()
      .from(Bucket)
      .findObject(objectName, 'id,version,metadata,user_metadata,created_at')
  } else {
    obj = await request.storage
      .from(Bucket)
      .findObject(objectName, 'id,version,metadata,user_metadata,created_at')
  }

  return request.storage.renderer(method).render(request, response, {
    bucket: Bucket,
    key: objectName,
    version: obj.version,
    object: obj,
  })
}

export async function publicRoutes(fastify: FastifyInstance) {
  fastify.head<getObjectRequestInterface>(
    '/public/:Bucket/*',
    {
      schema: {
        params: getObjectParamsSchema,
        summary: 'Get object info',
        description: 'returns object info',
        tags: ['object'],
        response: { '4xx': { $ref: 'errorSchema#' } },
      },
      config: {
        operation: { type: ROUTE_OPERATIONS.INFO_PUBLIC_OBJECT },
      },
    },
    async (request, response) => {
      return requestHandler(request, response, true)
    }
  )

  fastify.get<getObjectRequestInterface>(
    '/info/public/:Bucket/*',
    {
      exposeHeadRoute: false,
      schema: {
        params: getObjectParamsSchema,
        summary: 'Get object info',
        description: 'returns object info',
        tags: ['object'],
        response: { '4xx': { $ref: 'errorSchema#' } },
      },
      config: {
        operation: { type: ROUTE_OPERATIONS.INFO_PUBLIC_OBJECT },
      },
    },
    async (request, response) => {
      return requestHandler(request, response, true, 'info')
    }
  )
}

export async function authenticatedRoutes(fastify: FastifyInstance) {
  const summary = 'Retrieve object info'
  fastify.head<getObjectRequestInterface>(
    '/authenticated/:Bucket/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
      config: {
        operation: { type: 'object.head_authenticated_info' },
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )

  fastify.get<getObjectRequestInterface>(
    '/info/authenticated/:Bucket/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
      config: {
        operation: { type: 'object.get_authenticated_info' },
      },
    },
    async (request, response) => {
      return requestHandler(request, response, false, 'info')
    }
  )

  fastify.get<getObjectRequestInterface>(
    '/info/:Bucket/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        description: 'use HEAD /object/authenticated/{Bucket} instead',
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['deprecated'],
      },
      config: {
        operation: { type: 'object.get_authenticated_info' },
      },
    },
    async (request, response) => {
      return requestHandler(request, response, false, 'info')
    }
  )

  fastify.head<getObjectRequestInterface>(
    '/:Bucket/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        description: 'use HEAD /object/authenticated/{Bucket} instead',
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['deprecated'],
      },
      config: {
        operation: { type: 'object.head_authenticated_info' },
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )
}
