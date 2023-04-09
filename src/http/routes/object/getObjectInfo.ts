import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { IncomingMessage, Server, ServerResponse } from 'http'
import { getConfig } from '../../../config'
import { AuthenticatedRangeRequest } from '../../request'
import { Obj } from '../../../storage/schemas'

const { globalS3Bucket } = getConfig()

const getObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
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
  publicRoute = false
) {
  const { bucketName } = request.params
  const objectName = request.params['*']

  const s3Key = `${request.tenantId}/${bucketName}/${objectName}`

  let obj: Obj
  if (publicRoute) {
    obj = await request.storage.asSuperUser().from(bucketName).findObject(objectName, 'id,version')
  } else {
    obj = await request.storage.from(bucketName).findObject(objectName, 'id,version')
  }

  return request.storage.renderer('head').render(request, response, {
    bucket: globalS3Bucket,
    key: s3Key,
    version: obj.version,
  })
}

export async function publicRoutes(fastify: FastifyInstance) {
  fastify.head<getObjectRequestInterface>(
    '/public/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        summary: 'Get object info',
        description: 'returns object info',
        tags: ['object'],
        response: { '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      return requestHandler(request, response, true)
    }
  )

  fastify.get<getObjectRequestInterface>(
    '/info/public/:bucketName/*',
    {
      exposeHeadRoute: false,
      schema: {
        params: getObjectParamsSchema,
        summary: 'Get object info',
        description: 'returns object info',
        tags: ['object'],
        response: { '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      return requestHandler(request, response, true)
    }
  )
}

export async function authenticatedRoutes(fastify: FastifyInstance) {
  const summary = 'Retrieve object info'
  fastify.head<getObjectRequestInterface>(
    '/authenticated/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
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
    '/info/authenticated/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
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
    '/info/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        description: 'use HEAD /object/authenticated/{bucketName} instead',
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['deprecated'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )

  fastify.head<getObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        description: 'use HEAD /object/authenticated/{bucketName} instead',
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['deprecated'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )
}
