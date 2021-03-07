import { FastifyInstance } from 'fastify'
import { getPostgrestClient, getOwner } from '../../utils'
import { uploadObject, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { Obj, Bucket, AuthenticatedRequest } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const createObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string' },
    '*': { type: 'string' },
  },
  required: ['bucketName', '*'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    Key: { type: 'string' },
  },
  required: ['Key'],
}
interface createObjectRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof createObjectParamsSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Upload a new object'
  fastify.post<createObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema: {
        params: createObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { 200: successResponseSchema, '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      // check if the user is able to insert that row
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)
      const data = await request.file()

      // Can't seem to get the typing to work properly
      // https://github.com/fastify/fastify-multipart/issues/162
      /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
      const cacheTime = data.fields.cacheControl?.value
      const cacheControl: string = `max-age=${cacheTime}` ?? 'no-cache'

      const { bucketName } = request.params
      const objectName = request.params['*']

      const postgrest = getPostgrestClient(jwt)
      const owner = await getOwner(jwt)
      // @todo how to merge these into one query?
      // i can create a view and add INSTEAD OF triggers..is that the way to do it?
      // @todo add unique constraint for just bucket names
      const bucketResponse = await postgrest
        .from<Bucket>('buckets')
        .select('id')
        .eq('name', bucketName)
        .single()

      if (bucketResponse.error) {
        const { status, error } = bucketResponse
        console.log(error)
        return response.status(status).send({
          statusCode: 404,
          error: 'Not found',
          message: 'The requested bucket was not found',
        })
      }

      const { data: bucket } = bucketResponse

      const { data: results, error, status } = await postgrest
        .from<Obj>('objects')
        .insert(
          [
            {
              name: objectName,
              owner: owner,
              bucketId: bucket.id,
              metadata: {
                mimetype: data.mimetype,
                cacheControl,
              },
            },
          ],
          {
            returning: 'minimal',
          }
        )
        .single()

      console.log(results, error)
      if (error) {
        return response.status(status).send({
          statusCode: error.code,
          error: error.details,
          message: error.message,
        })
      }

      // if successfully inserted, upload to s3
      const s3Key = `${projectRef}/${bucketName}/${objectName}`
      const uploadResult = await uploadObject(
        client,
        globalS3Bucket,
        s3Key,
        data.file,
        data.mimetype,
        cacheControl
      )

      return response.status(uploadResult.$metadata.httpStatusCode ?? 200).send({
        Key: s3Key,
      })
    }
  )
}
