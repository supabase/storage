import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { Bucket } from '../../types/types'
import { transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { normalizeContentType } from '../../utils'
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

const getPublicObjectParamsSchema = {
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

interface getObjectRequestInterface {
  Params: FromSchema<typeof getPublicObjectParamsSchema>
  Headers: {
    range?: string
  }
  Querystring: FromSchema<typeof getObjectQuerySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object from a public bucket'
  fastify.get<getObjectRequestInterface>(
    '/public/:bucketName/*',
    {
      // @todo add success response schema here
      schema: {
        params: getPublicObjectParamsSchema,
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const objectName = request.params['*']
      const { download } = request.query

      const { error, status } = await request.superUserPostgrest
        .from<Bucket>('buckets')
        .select('id, public')
        .eq('id', bucketName)
        .eq('public', true)
        .single()

      if (error) {
        request.log.error({ error }, 'error finding public bucket')
        return response.status(400).send(transformPostgrestError(error, status))
      }

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
          .header('Content-Length', data.metadata.contentLength)
          .header('ETag', data.metadata.eTag)
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
        if (err.$metadata?.httpStatusCode === 404) {
          return response.status(404).send()
        } else {
          return response.status(400).send({
            message: err.message,
            statusCode: '400',
            error: err.message,
          })
        }
      }
    }
  )
}
