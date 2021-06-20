import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest, Obj, ObjectMetadata } from '../../types/types'
import { getOwner, getPostgrestClient, isValidKey, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'
import { deleteObject, headObject, initClient, uploadObject } from '../../utils/s3'

const { region, projectRef, globalS3Bucket, globalS3Endpoint, serviceKey } = getConfig()
const client = initClient(region, globalS3Endpoint)

const createObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', example: 'avatars' },
    '*': { type: 'string', example: 'folder/cat.png' },
  },
  required: ['bucketName', '*'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    Key: {
      type: 'string',
      example: 'avatars/folder/cat.png',
    },
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
    tags: ['object'],
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
      const superUserPostgrest = getPostgrestClient(serviceKey)

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

      const isUpsert =
        request.headers['x-upsert'] && request.headers['x-upsert'] === 'true' ? true : false

      if (isUpsert) {
        const { data: results, error, status } = await postgrest
          .from<Obj>('objects')
          .upsert(
            [
              {
                name: objectName,
                owner: owner,
                bucket_id: bucketName,
              },
            ],
            {
              onConflict: 'name, bucket_id',
            }
          )
          .single()

        if (error) {
          request.log.error({ error }, 'error object')
          return response.status(400).send(transformPostgrestError(error, status))
        }
        request.log.info({ results }, 'results')
      } else {
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
      }

      // if successfully inserted, upload to s3
      const path = `${bucketName}/${objectName}`
      const s3Key = `${projectRef}/${path}`
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
      const { error: updateError, status: updateStatus } = await superUserPostgrest
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
        Key: path,
      })
    }
  )
}
