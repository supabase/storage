import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { Bucket } from '../../types/types'
import { getPostgrestClient, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { getObject, initClient } from '../../utils/s3'

const { region, projectRef, globalS3Bucket, globalS3Endpoint, serviceKey } = getConfig()
const client = initClient(region, globalS3Endpoint)

const getPublicObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', example: 'avatars' },
    '*': { type: 'string', example: 'folder/cat.png' },
  },
  required: ['bucketName', '*'],
} as const
interface getObjectRequestInterface {
  Params: FromSchema<typeof getPublicObjectParamsSchema>
  Headers: {
    range?: string
  }
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
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const objectName = request.params['*']
      const range = request.headers.range

      const superUserPostgrest = getPostgrestClient(serviceKey)
      const { error, status } = await superUserPostgrest
        .from<Bucket>('buckets')
        .select('id, public')
        .eq('id', bucketName)
        .eq('public', true)
        .single()

      if (error) {
        request.log.error({ error }, 'error finding public bucket')
        return response.status(400).send(transformPostgrestError(error, status))
      }

      const s3Key = `${projectRef}/${bucketName}/${objectName}`
      request.log.info(s3Key)
      try {
        const data = await getObject(client, globalS3Bucket, s3Key, range)
        response
          .status(data.$metadata.httpStatusCode ?? 200)
          .header('Content-Type', data.ContentType)
          .header('Cache-Control', data.CacheControl)
          .header('ETag', data.ETag)
          .header('Last-Modified', data.LastModified)
        if (data.ContentRange) {
          response.header('Content-Range', data.ContentRange)
        }
        return response.send(data.Body)
      } catch (err) {
        if (err.$metadata?.httpStatusCode === 404) {
          return response.status(404).send()
        } else {
          return response.status(400).send({
            message: err.message,
            statusCode: err.$metadata?.httpStatusCode,
            error: err.message,
          })
        }
      }
    }
  )
}
