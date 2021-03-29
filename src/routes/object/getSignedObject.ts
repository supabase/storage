import { FastifyInstance } from 'fastify'
import { verifyJWT } from '../../utils/'
import { getObject, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { signedToken } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const getSignedObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string' },
    '*': { type: 'string' },
  },
  required: ['bucketName', '*'],
} as const
const getSignedObjectQSSchema = {
  type: 'object',
  properties: {
    token: { type: 'string' },
  },
  required: ['token'],
} as const
interface getSignedObjectRequestInterface {
  Params: FromSchema<typeof getSignedObjectParamsSchema>
  Querystring: FromSchema<typeof getSignedObjectQSSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object via a presigned URL'
  fastify.get<getSignedObjectRequestInterface>(
    '/sign/:bucketName/*',
    {
      schema: {
        params: getSignedObjectParamsSchema,
        querystring: getSignedObjectQSSchema,
        summary,
        response: { '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      const { token } = request.query
      try {
        const payload = await verifyJWT(token)
        const { url } = payload as signedToken
        const s3Key = `${projectRef}/${url}`
        request.log.info(s3Key)
        const data = await getObject(client, globalS3Bucket, s3Key)

        return response
          .status(data.$metadata.httpStatusCode ?? 200)
          .header('Content-Type', data.ContentType)
          .header('Cache-Control', data.CacheControl ?? 'no-cache')
          .header('ETag', data.ETag)
          .header('Last-Modified', data.LastModified)
          .send(data.Body)
      } catch (err) {
        request.log.error(err)
        return response.status(400).send({
          statusCode: '400',
          error: err.name,
          message: err.message,
        })
      }
    }
  )
}
