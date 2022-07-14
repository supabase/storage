import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { SignedToken } from '../../types/types'
import { getJwtSecret, verifyJWT } from '../../utils/'
import { getConfig } from '../../utils/config'
import { normalizeContentType } from '../../utils'
import { createResponse } from '../../utils/generic-routes'
import { S3Backend } from '../../backend/s3'
import { FileBackend } from '../../backend/file'
import { GenericStorageBackend } from '../../backend/generic'

const {
  region,
  globalS3Bucket,
  globalS3Endpoint,
  storageBackendType,
  globalS3AccessKeyId,
  globalS3SecretAccessKey,
} = getConfig()
let storageBackend: GenericStorageBackend

if (storageBackendType === 'file') {
  storageBackend = new FileBackend()
} else {
  storageBackend = new S3Backend(
    region,
    globalS3Endpoint,
    globalS3AccessKeyId,
    globalS3SecretAccessKey
  )
}

const getSignedObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', example: 'avatars' },
    '*': { type: 'string', example: 'folder/cat.png' },
  },
  required: ['bucketName', '*'],
} as const
const getSignedObjectQSSchema = {
  type: 'object',
  properties: {
    token: {
      type: 'string',
      example:
        'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cmwiOiJidWNrZXQyL3B1YmxpYy9zYWRjYXQtdXBsb2FkMjMucG5nIiwiaWF0IjoxNjE3NzI2MjczLCJleHAiOjE2MTc3MjcyNzN9.uBQcXzuvXxfw-9WgzWMBfE_nR3VOgpvfZe032sfLSSk',
    },
  },
  required: ['token'],
} as const

interface GetSignedObjectRequestInterface {
  Params: FromSchema<typeof getSignedObjectParamsSchema>
  Querystring: FromSchema<typeof getSignedObjectQSSchema>
  Headers: {
    range?: string
  }
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object via a presigned URL'
  fastify.get<GetSignedObjectRequestInterface>(
    '/sign/:bucketName/*',
    {
      // @todo add success response schema here
      schema: {
        params: getSignedObjectParamsSchema,
        querystring: getSignedObjectQSSchema,
        summary,
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      const { token } = request.query
      try {
        const jwtSecret = await getJwtSecret(request.tenantId)
        const payload = await verifyJWT(token, jwtSecret)
        const { url } = payload as SignedToken
        const s3Key = `${request.tenantId}/${url}`
        request.log.info(s3Key)
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
          .header('Content-Length', data.metadata.contentLength)
          .header('ETag', data.metadata.eTag)
          .header('Last-Modified', data.metadata.lastModified)
        if (data.metadata.contentRange) {
          response.header('Content-Range', data.metadata.contentRange)
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
  )
}
