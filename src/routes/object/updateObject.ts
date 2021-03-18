import { FastifyInstance } from 'fastify'
import { getPostgrestClient, getOwner, transformPostgrestError } from '../../utils'
import { uploadObject, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { Obj, AuthenticatedRequest } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const updateObjectParamsSchema = {
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
interface updateObjectRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof updateObjectParamsSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Update the object at an existing key'
  fastify.put<updateObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema: {
        params: updateObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { 200: successResponseSchema, '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      // check if the user is able to update the row
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)
      const data = await request.file()
      /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
      const cacheTime = data.fields.cacheControl?.value
      // @todo maintain the old cache time if nothing is provided here
      const cacheControl: string = cacheTime ? `max-age=${cacheTime}` : 'no-cache'

      const { bucketName } = request.params
      const objectName = request.params['*']

      const postgrest = getPostgrestClient(jwt)
      const owner = await getOwner(jwt)

      const objectResponse = await postgrest
        .from<Obj>('objects')
        .update({
          last_accessed_at: new Date().toISOString(),
          owner,
          metadata: {
            mimetype: data.mimetype,
            cacheControl,
          },
        })
        .match({ bucket_id: bucketName, name: objectName })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        console.log(error)
        return response.status(400).send(transformPostgrestError(error, status))
      }

      // if successfully inserted, upload to s3
      const s3Key = `${projectRef}/${bucketName}/${objectName}`

      // @todo adding contentlength metadata will be harder since everything is streams
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
