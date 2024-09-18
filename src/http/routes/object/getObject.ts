import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { IncomingMessage, Server, ServerResponse } from 'http'
import { getConfig } from '../../../config'
import { AuthenticatedRangeRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'
import { ERRORS } from '@internal/errors'
import { Obj } from '@storage/schemas'

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

  // send the object from s3
  const s3Key = `${request.tenantId}/${bucketName}/${objectName}`
  const bucket = await request.storage.asSuperUser().findBucket(bucketName, 'id,public', {
    dontErrorOnEmpty: true,
  })

  // The request is not authenticated
  if (!request.isAuthenticated) {
    // The bucket must be public to access its content
    if (!bucket?.public) {
      throw ERRORS.NoSuchBucket(bucketName)
    }
  }

  // The request is authenticated
  if (!bucket) {
    throw ERRORS.NoSuchBucket(bucketName)
  }

  let obj: Obj | undefined

  if (bucket.public) {
    // request is authenticated but we still use the superUser as we don't need to check RLS
    obj = await request.storage.asSuperUser().from(bucketName).findObject(objectName, 'id, version')
  } else {
    // request is authenticated use RLS
    obj = await request.storage.from(bucketName).findObject(objectName, 'id, version')
  }

  return request.storage.renderer('asset').render(request, response, {
    bucket: storageS3Bucket,
    key: s3Key,
    version: obj.version,
    download,
    signal: request.signals.disconnect.signal,
  })
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object'
  fastify.get<getObjectRequestInterface>(
    '/authenticated/:bucketName/*',
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
    '/:bucketName/*',
    {
      exposeHeadRoute: false,
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        summary: 'Get object',
        description: 'Serve objects',
        tags: ['object'],
        response: { '4xx': { $ref: 'errorSchema#' } },
      },
      config: {
        operation: { type: ROUTE_OPERATIONS.GET_AUTH_OBJECT },
        allowInvalidJwt: true,
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )
}
