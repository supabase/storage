import { FastifyInstance } from 'fastify'
import { getPostgrestClient, getOwner, transformPostgrestError, isValidKey } from '../../utils'
import { uploadObject, initClient, deleteObject, headObject } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { Obj, AuthenticatedRequest, ObjectMetadata } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'

const { region, projectRef, globalS3Bucket, globalS3Endpoint, serviceKey } = getConfig()
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

  const schema = createDefaultSchema(successResponseSchema, {
    params: createObjectParamsSchema,
    summary,
  })

  fastify.post<createObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema,
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
      const cacheControl: string = cacheTime ? `max-age=${cacheTime}` : 'no-cache'

      const { bucketName } = request.params
      const objectName = request.params['*']

      if (!isValidKey(objectName) || !isValidKey(bucketName)) {
        return response
          .status(400)
          .send(createResponse('The key contains invalid characters', '400', 'Invalid key'))
      }

      const postgrest = getPostgrestClient(jwt)
      let owner
      try {
        owner = await getOwner(jwt)
      } catch (err) {
        request.log.error(err)
        return response.status(400).send({
          statusCode: '400',
          error: err.message,
          message: err.message,
        })
      }

      const { data: results, error, status } = await postgrest
        .from<Obj>('objects')
        .insert(
          [
            {
              name: objectName,
              owner: owner,
              bucket_id: bucketName,
            },
          ],
          {
            returning: 'minimal',
          }
        )
        .single()

      if (error) {
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      request.log.info({ results }, 'results')

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
        // undo operations as super user
        const superUserPostgrest = getPostgrestClient(serviceKey)
        await superUserPostgrest
          .from<Obj>('objects')
          .delete()
          .match({
            name: objectName,
            bucket_id: bucketName,
          })
          .single()
        await deleteObject(client, globalS3Bucket, s3Key)

        // return an error response
        return response
          .status(400)
          .send(
            createResponse(
              'The object exceeded the maximum allowed size',
              '413',
              'Payload too large'
            )
          )
      }

      const objectMetadata = await headObject(client, globalS3Bucket, s3Key)
      // update content-length as super user since user may not have update permissions
      const metadata: ObjectMetadata = {
        mimetype: data.mimetype,
        cacheControl,
        size: objectMetadata.ContentLength,
      }
      const { error: updateError, status: updateStatus } = await getPostgrestClient(serviceKey)
        .from<Obj>('objects')
        .update({
          metadata,
        })
        .match({ bucket_id: bucketName, name: objectName })
        .single()

      if (updateError) {
        request.log.error({ error: updateError }, 'update error')
        return response.status(400).send(transformPostgrestError(updateError, updateStatus))
      }

      return response.status(uploadResult.$metadata.httpStatusCode ?? 200).send({
        Key: s3Key,
      })
    }
  )
}
