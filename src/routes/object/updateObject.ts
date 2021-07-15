import { ServiceOutputTypes } from '@aws-sdk/client-s3'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { Obj, ObjectMetadata } from '../../types/types'
import { getOwner, getPostgrestClient, isValidKey, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'
import { S3Backend } from '../../backend/s3'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const storageBackend = new S3Backend(region, globalS3Endpoint)

const updateObjectParamsSchema = {
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
    Key: { type: 'string', example: 'projectref/avatars/folder/cat.png' },
  },
  required: ['Key'],
}
interface updateObjectRequestInterface extends RequestGenericInterface {
  Params: FromSchema<typeof updateObjectParamsSchema>
  Headers: {
    authorization: string
    'content-type': string
    'cache-control'?: string
    'x-upsert'?: string
  }
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Update the object at an existing key'

  const schema = createDefaultSchema(successResponseSchema, {
    params: updateObjectParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.addContentTypeParser(
    ['application/json', 'text/plain'],
    function (request, payload, done) {
      done(null)
    }
  )

  fastify.put<updateObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema,
    },
    async (request, response) => {
      // check if the user is able to update the row
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const contentType = request.headers['content-type']
      request.log.info(`content-type is ${contentType}`)

      const { bucketName } = request.params
      const objectName = request.params['*']
      const path = `${bucketName}/${objectName}`
      const s3Key = `${projectRef}/${path}`
      let mimeType: string, cacheControl: string, isTruncated: boolean
      let uploadResult: ServiceOutputTypes

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
        console.log(err)
        return response.status(400).send(createResponse(err.message, '400', err.message))
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

      if (contentType?.startsWith('multipart/form-data')) {
        const data = await request.file()
        /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
        const cacheTime = data.fields.cacheControl?.value
        cacheControl = cacheTime ? `max-age=${cacheTime}` : 'no-cache'
        mimeType = data.mimetype

        uploadResult = await storageBackend.uploadObject(
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
        isTruncated = data.file.truncated
      } else {
        // just assume its a binary file
        mimeType = request.headers['content-type']
        cacheControl = request.headers['cache-control'] ?? 'no-cache'

        uploadResult = await storageBackend.uploadObject(
          globalS3Bucket,
          s3Key,
          request.raw,
          mimeType,
          cacheControl
        )
        const { fileSizeLimit } = getConfig()
        // @todo more secure to get this from the stream or from s3 in the next step
        isTruncated = Number(request.headers['content-length']) > fileSizeLimit
      }

      if (isTruncated) {
        // @todo tricky to handle since we need to undo the s3 upload
      }

      const objectMetadata = await storageBackend.headObject(globalS3Bucket, s3Key)
      // update content-length as super user since user may not have update permissions
      const metadata: ObjectMetadata = {
        mimetype: mimeType,
        cacheControl,
        size: objectMetadata.ContentLength,
      }
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
        Key: path,
      })
    }
  )
}
