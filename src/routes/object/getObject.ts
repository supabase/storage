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

  if (!isValidKey(objectName) || !isValidKey(bucketName)) {
    return response
      .status(400)
      .send(createResponse('The key contains invalid characters', '400', 'Invalid key'))
  }

  const objectResponse = await request.postgrest
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

  // send the object from s3
  const s3Key = `${request.tenantId}/${bucketName}/${objectName}`
  request.log.info(s3Key)
  try {
    const data = await storageBackend.getObject(globalS3Bucket, s3Key, {
      ifModifiedSince: request.headers['if-modified-since'],
      ifNoneMatch: request.headers['if-none-match'],
      range: request.headers.range,
    })

    response
      .status(data.metadata.httpStatusCode ?? 200)
      .header('Accept-Ranges', 'bytes')
      .header('Content-Type', normalizeContentType(data.metadata.mimetype))
      .header('Cache-Control', data.metadata.cacheControl)
      .header('ETag', data.metadata.eTag)
      .header('Content-Length', data.metadata.contentLength)
      .header('Last-Modified', data.metadata.lastModified?.toUTCString())
    if (data.metadata.contentRange) {
      response.header('Content-Range', data.metadata.contentRange)
    }

    if (typeof download !== 'undefined') {
      if (download === '') {
        response.header('Content-Disposition', 'attachment;')
      } else {
        const encodedFileName = encodeURIComponent(download)

        response.header(
          'Content-Disposition',
          `attachment; filename=${encodedFileName}; filename*=UTF-8''${encodedFileName};`
        )
      }
    }

    return response.send(data.body)
  } catch (err: any) {
    if (err.$metadata?.httpStatusCode === 304) {
      return response.status(304).send()
    }
    request.log.error(err)
    return response.status(400).send(createResponse(err.message, '400', err.name))
  }
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object'
  fastify.get<getObjectRequestInterface>(
    '/authenticated/:bucketName/*',
    {
      exposeHeadRoute: true,
      // @todo add success response schema here
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
    '/:bucketName/*',
    {
      exposeHeadRoute: true,
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
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
