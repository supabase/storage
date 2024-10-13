import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { IncomingMessage, Server, ServerResponse } from 'http'
import { getConfig } from '../../../config'
import { AuthenticatedRangeRequest } from '../../types'
import { Obj } from '@storage/schemas'
import { ROUTE_OPERATIONS } from '../operations'
import { ERRORS } from '@internal/errors'

const { storageS3Bucket } = getConfig()

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
  publicRoute = false,
  method: 'head' | 'info' = 'head'
) {
  const { bucketName } = request.params
  const objectName = request.params['*']

  const s3Key = `${request.tenantId}/${bucketName}/${objectName}`

  const bucket = await request.storage.asSuperUser().findBucket(bucketName, 'id,public', {
    dontErrorOnEmpty: true,
  })

  // Not Authenticated flow
  if (!request.isAuthenticated) {
    if (!bucket?.public) {
      throw ERRORS.NoSuchBucket(bucketName)
    }
  }

  // Authenticated flow
  if (!bucket) {
    throw ERRORS.NoSuchBucket(bucketName)
  }

  let obj: Obj

  if (bucket.public || publicRoute) {
    obj = await request.storage
      .asSuperUser()
      .from(bucketName)
      .findObject(objectName, 'id,name,version,metadata,user_metadata,created_at')
  } else {
    obj = await request.storage
      .from(bucketName)
      .findObject(objectName, 'id,name,version,metadata,user_metadata,created_at')
  }

  return request.storage.renderer(method).render(request, response, {
    bucket: storageS3Bucket,
    key: s3Key,
    version: obj.version,
    object: obj,
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
      config: {
        operation: { type: ROUTE_OPERATIONS.INFO_PUBLIC_OBJECT },
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
    '/authenticated/:bucketName/*',
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
    '/info/authenticated/:bucketName/*',
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
    '/info/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        summary,
        description: 'Object Info',
        tags: ['object'],
        response: { '4xx': { $ref: 'errorSchema#' } },
      },
      config: {
        operation: { type: 'object.get_authenticated_info' },
        allowInvalidJwt: true,
      },
    },
    async (request, response) => {
      return requestHandler(request, response, false, 'info')
    }
  )

  fastify.head<getObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        summary,
        description: 'Head object info',
        tags: ['object'],
        response: { '4xx': { $ref: 'errorSchema#' } },
      },
      config: {
        operation: { type: 'object.head_authenticated_info' },
        allowInvalidJwt: true,
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )
}
