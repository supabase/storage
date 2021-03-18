import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getPostgrestClient, isValidKey, transformPostgrestError } from '../../utils'
import { getObject, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { AuthenticatedRequest, Obj } from '../../types/types'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const getObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string' },
    '*': { type: 'string' },
  },
  required: ['bucketName', '*'],
} as const
interface getObjectRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getObjectParamsSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object'
  fastify.get<getObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const postgrest = getPostgrestClient(jwt)

      const { bucketName } = request.params
      const objectName = request.params['*']

      if (!isValidKey(objectName) || !isValidKey(bucketName)) {
        return response.status(400).send({
          statusCode: '400',
          error: 'Invalid key',
          message: 'The key contains invalid characters',
        })
      }

      const objectResponse = await postgrest
        .from<Obj>('objects')
        .select('*')
        .match({
          name: objectName,
          bucket_id: bucketName,
        })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        console.log(error)
        return response.status(400).send(transformPostgrestError(error, status))
      }

      // send the object from s3
      const s3Key = `${projectRef}/${bucketName}/${objectName}`
      console.log(s3Key)
      const data = await getObject(client, globalS3Bucket, s3Key)

      return response
        .status(data.$metadata.httpStatusCode ?? 200)
        .header('Content-Type', data.ContentType)
        .header('Cache-Control', data.CacheControl)
        .header('ETag', data.ETag)
        .header('Last-Modified', data.LastModified)
        .send(data.Body)
    }
  )
}
