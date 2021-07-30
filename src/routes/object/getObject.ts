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
    bucketName: { type: 'string', example: 'avatars' },
    '*': { type: 'string', example: 'folder/cat.png' },
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
  >
) {
  const range = request.headers.range

  const { bucketName } = request.params
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
  const s3Key = `${request.projectRef}/${bucketName}/${objectName}`
  request.log.info(s3Key)
  try {
    const data = await storageBackend.getObject(globalS3Bucket, s3Key, range)

    response
      .status(data.metadata.httpStatusCode ?? 200)
      .header('Content-Type', normalizeContentType(data.metadata.mimetype))
      .header('Cache-Control', data.metadata.cacheControl)
      .header('ETag', data.metadata.eTag)
      .header('Last-Modified', data.metadata.lastModified)
    if (data.metadata.contentRange) {
      response.header('Content-Range', data.metadata.contentRange)
    }
    return response.send(data.body)
  } catch (err) {
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
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )

  // to be deprecated
  fastify.get<getObjectRequestInterface>(
    '/:bucketName/*',
    {
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary:
          'Deprecated (use GET /object/authenticated/{bucketName} instead): Retrieve an object',
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )
}
