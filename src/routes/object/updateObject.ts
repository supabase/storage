import { FastifyInstance } from 'fastify'
import { getPostgrestClient, getOwner, transformPostgrestError, isValidKey } from '../../utils'
import { uploadObject, initClient, headObject } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { Obj, AuthenticatedRequest, ObjectMetadata } from '../../types/types'
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
      const cacheControl: string = cacheTime ? `max-age=${cacheTime}` : 'no-cache'
      const metadata: ObjectMetadata = {
        mimetype: data.mimetype,
      }
      if (cacheTime) {
        metadata.cacheControl = `max-age=${cacheTime}`
      }

      const { bucketName } = request.params
      const objectName = request.params['*']

      if (!isValidKey(objectName) || !isValidKey(bucketName)) {
        return response.status(400).send({
          statusCode: '400',
          error: 'Invalid key',
          message: 'The key contains invalid characters',
        })
      }

      const postgrest = getPostgrestClient(jwt)
      let owner
      try {
        owner = await getOwner(jwt)
      } catch (err) {
        console.log(err)
        return response.status(400).send({
          statusCode: '400',
          error: err.message,
          message: err.message,
        })
      }

      const objectResponse = await postgrest
        .from<Obj>('objects')
        .update({
          last_accessed_at: new Date().toISOString(),
          owner,
        })
        .match({ bucket_id: bucketName, name: objectName })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
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

      // since we are using streams, fastify can't throw the error reliably
      // busboy sets the truncated property on streams if limit was exceeded
      // https://github.com/fastify/fastify-multipart/issues/196#issuecomment-782847791
      /* @ts-expect-error: busboy doesn't export proper types */
      const isTruncated = data.file.truncated
      if (isTruncated) {
        // @todo tricky to handle since we need to undo the s3 upload
      }

      const objectMetadata = await headObject(client, globalS3Bucket, s3Key)
      // update content-length as super user since user may not have update permissions
      metadata.size = objectMetadata.ContentLength
      const { error: updateError, status: updateStatus } = await postgrest
        .from<Obj>('objects')
        .update({
          metadata,
        })
        .match({ bucket_id: bucketName, name: objectName })
        .single()

      if (updateError) {
        request.log.error({ error: updateError }, 'error bucket')
        return response.status(400).send(transformPostgrestError(updateError, updateStatus))
      }

      return response.status(uploadResult.$metadata.httpStatusCode ?? 200).send({
        Key: s3Key,
      })
    }
  )
}