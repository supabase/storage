import { getConfig } from '../../utils/config'
import { GenericStorageBackend } from '../../backend/generic'
import { FileBackend } from '../../backend/file'
import { S3Backend } from '../../backend/s3'
import { FromSchema } from 'json-schema-to-ts'
import { FastifyInstance } from 'fastify'
import { Bucket } from '../../types/types'
import { normalizeContentType, transformPostgrestError } from '../../utils'
import { Imgproxy } from '../../renderer/imgproxy'

const { region, globalS3Bucket, globalS3Endpoint, storageBackendType, imgProxyURL } = getConfig()

let storageBackend: GenericStorageBackend

if (storageBackendType === 'file') {
  storageBackend = new FileBackend()
} else {
  storageBackend = new S3Backend(region, globalS3Endpoint)
}

const imageRenderer = new Imgproxy(storageBackend, {
  url: imgProxyURL || '',
})

const renderPublicImageParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
} as const

const renderImageQuerySchema = {
  type: 'object',
  properties: {
    height: { type: 'number', examples: [100] },
    width: { type: 'number', examples: [100] },
    resize: { type: 'string', enum: ['fill', 'fit', 'fill-down', 'force', 'auto'] },
  },
} as const

interface renderImageRequestInterface {
  Params: FromSchema<typeof renderPublicImageParamsSchema>
  Querystring: FromSchema<typeof renderImageQuerySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Render a public image with the given transformations'
  fastify.get<renderImageRequestInterface>(
    '/public/:bucketName/*',
    {
      schema: {
        params: renderPublicImageParamsSchema,
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const objectName = request.params['*']

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

      try {
        const { response: imageResponse, urlTransformation } = await imageRenderer.transform(
          globalS3Bucket,
          s3Key,
          request.query
        )

        response
          .status(imageResponse.status)
          .header('Accept-Ranges', 'bytes')
          .header('Content-Type', normalizeContentType(imageResponse.headers['content-type']))
          .header('Cache-Control', imageResponse.headers['cache-control'])
          .header('Content-Length', imageResponse.headers['content-length'])
          .header('ETag', imageResponse.headers['etag'])
          .header('X-Transformations', urlTransformation.join(','))

        return response.send(imageResponse.data)
      } catch (err: any) {
        if (err.response) {
          return response.status(err.response?.status || 500).send({
            message: err.message,
            statusCode: err.response?.status || '500',
            error: err.message,
          })
        }

        return response.status(500).send({
          message: err.message,
          statusCode: '500',
          error: err.message,
        })
      }
    }
  )
}
