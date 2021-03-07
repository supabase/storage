import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getPostgrestClient } from '../../utils'
import { getObject, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { Obj } from '../../types/types'

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
interface getObjectRequestInterface {
  Params: FromSchema<typeof getObjectParamsSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  fastify.get<getObjectRequestInterface>(
    '/:bucketName/*',
    { schema: { params: getObjectParamsSchema } },
    async (request, response) => {
      const authHeader = request.headers.authorization
      if (!authHeader) {
        return response.status(403).send('Go away')
      }
      const jwt = authHeader.substring('Bearer '.length)

      const postgrest = getPostgrestClient(jwt)

      const { bucketName } = request.params
      const objectName = request.params['*']

      const objectResponse = await postgrest
        .from<Obj>('objects')
        .select('*, buckets(*)')
        .match({
          name: objectName,
          'buckets.name': bucketName,
        })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        console.log(error)
        return response.status(status).send(error.message)
      }
      const { data: results } = objectResponse

      if (!results.buckets) {
        // @todo why is this check necessary?
        // if corresponding bucket is not found, i want the object also to not be returned
        // is it cos of https://github.com/PostgREST/postgrest/issues/1075 ?
        return response.status(404).send('not found')
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
