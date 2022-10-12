import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { IncomingMessage, Server, ServerResponse } from 'http'
import { AuthenticatedRangeRequest, Obj } from '../../types/types'
import { isValidKey, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { normalizeContentType } from '../../utils'
import { createResponse } from '../../utils/generic-routes'
import { S3Backend } from '../../backend/s3'
import { FileBackend } from '../../backend/file'
import { GenericStorageBackend } from '../../backend/generic'

const { region, globalS3Bucket, globalS3Endpoint, storageBackendType } = getConfig()
let storageBackend: GenericStorageBackend

if (storageBackendType === 'file') {
  storageBackend = new FileBackend()
} else {
  storageBackend = new S3Backend(region, globalS3Endpoint)
}

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

  if (!isValidKey(objectName) || !isValidKey(bucketName)) {
    return response
      .status(400)
      .send(createResponse('The key contains invalid characters', '400', 'Invalid key'))
  }

  const postgrest = publicRoute ? request.superUserPostgrest : request.postgrest
  const objectResponse = await postgrest
    .from<Obj>('objects')
    .select('id')
    .match({
      name: objectName,
      bucket_id: bucketName,
    })
    .single()

  if (objectResponse.error) {
    const { status, error } = objectResponse
    request.log.error({ error }, 'error object')
    return response.status(400).send(transformPostgrestError(error, status))
  }

  const s3Key = `${request.tenantId}/${bucketName}/${objectName}`

  try {
    const data = await storageBackend.headObject(globalS3Bucket, s3Key)

    response
      .status(data.httpStatusCode ?? 200)
      .header('Content-Type', normalizeContentType(data.mimetype))
      .header('Cache-Control', data.cacheControl)
      .header('Content-Length', data.size)
      .header('ETag', data.eTag)
      .header('Last-Modified', data.lastModified?.toUTCString())

    return response.send()
  } catch (err: any) {
    if (err.$metadata?.httpStatusCode === 304) {
      return response.status(304).send()
    }
    request.log.error(err)
    return response.status(400).send(createResponse(err.message, '400', err.name))
  }
}

export async function publicRoutes(fastify: FastifyInstance) {
  fastify.head<getObjectRequestInterface>(
    '/public/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary: 'Get object info',
        description: 'returns object info',
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
