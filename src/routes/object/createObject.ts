import { PostgrestSingleResponse } from '@supabase/postgrest-js/dist/main/lib/types'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { Obj, ObjectMetadata } from '../../types/types'
import {
  getFileSizeLimit,
  getJwtSecret,
  getOwner,
  isValidKey,
  transformPostgrestError,
} from '../../utils'
import { getConfig } from '../../utils/config'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'
import { S3Backend } from '../../backend/s3'
import { FileBackend } from '../../backend/file'
import { GenericStorageBackend } from '../../backend/generic'
import { StorageBackendError } from '../../utils/errors'

const { region, globalS3Bucket, globalS3Endpoint, storageBackendType } = getConfig()
let storageBackend: GenericStorageBackend

if (storageBackendType === 'file') {
  storageBackend = new FileBackend()
} else {
  storageBackend = new S3Backend(region, globalS3Endpoint)
}

const createObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    Key: {
      type: 'string',
      examples: ['avatars/folder/cat.png'],
    },
  },
  required: ['Key'],
}
interface createObjectRequestInterface extends RequestGenericInterface {
  Params: FromSchema<typeof createObjectParamsSchema>
  Headers: {
    authorization: string
    'content-type': string
    'cache-control'?: string
    'x-upsert'?: string
  }
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Upload a new object'

  const schema = createDefaultSchema(successResponseSchema, {
    params: createObjectParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.addContentTypeParser(
    ['application/json', 'text/plain'],
    function (request, payload, done) {
      done(null)
    }
  )

  fastify.post<createObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema,
    },
    async (request, response) => {
      const contentType = request.headers['content-type']
      request.log.info(`content-type is ${contentType}`)

      const { bucketName } = request.params
      const objectName = request.params['*']
      const path = `${bucketName}/${objectName}`
      const s3Key = `${request.tenantId}/${path}`
      let mimeType: string, cacheControl: string
      let isTruncated = false
      let uploadResult: ObjectMetadata

      if (!isValidKey(objectName) || !isValidKey(bucketName)) {
        return response
          .status(400)
          .send(createResponse('The key contains invalid characters', '400', 'Invalid key'))
      }

      const jwtSecret = await getJwtSecret(request.tenantId)
      let owner
      try {
        owner = await getOwner(request.jwt, jwtSecret)
      } catch (err: any) {
        request.log.error(err)
        return response.status(400).send({
          statusCode: '400',
          error: err.message,
          message: err.message,
        })
      }

      const isUpsert =
        request.headers['x-upsert'] && request.headers['x-upsert'] === 'true' ? true : false

      let postgrestResponse: PostgrestSingleResponse<Obj>

      if (isUpsert) {
        postgrestResponse = await request.postgrest
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
              returning: 'minimal',
            }
          )
          .single()
      } else {
        postgrestResponse = await request.postgrest
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
      }

      const { error, status, data: results } = postgrestResponse

      if (error) {
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      request.log.info({ results }, 'results')

      const fileSizeLimit = await getFileSizeLimit(request.tenantId)

      // if successfully inserted, upload to s3
      if (contentType?.startsWith('multipart/form-data')) {
        const data = await request.file({ limits: { fileSize: fileSizeLimit } })

        // Can't seem to get the typing to work properly
        // https://github.com/fastify/fastify-multipart/issues/162
        /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
        const cacheTime = data.fields.cacheControl?.value
        cacheControl = cacheTime ? `max-age=${cacheTime}` : 'no-cache'
        mimeType = data.mimetype
        try {
          uploadResult = await storageBackend.uploadObject(
            globalS3Bucket,
            s3Key,
            data.file,
            mimeType,
            cacheControl
          )
          // since we are using streams, fastify can't throw the error reliably
          // busboy sets the truncated property on streams if limit was exceeded
          // https://github.com/fastify/fastify-multipart/issues/196#issuecomment-782847791
          isTruncated = data.file.truncated
        } catch (err) {
          return await handleUploadError(err as StorageBackendError)
        }
      } else {
        // just assume its a binary file
        mimeType = request.headers['content-type']
        cacheControl = request.headers['cache-control'] ?? 'no-cache'

        try {
          uploadResult = await storageBackend.uploadObject(
            globalS3Bucket,
            s3Key,
            request.raw,
            mimeType,
            cacheControl
          )
          // @todo more secure to get this from the stream or from s3 in the next step
          isTruncated = Number(request.headers['content-length']) > fileSizeLimit
        } catch (err) {
          return await handleUploadError(err as StorageBackendError)
        }
      }

      if (isTruncated) {
        // undo operations as super user
        await request.superUserPostgrest
          .from<Obj>('objects')
          .delete()
          .match({
            name: objectName,
            bucket_id: bucketName,
          })
          .single()
        await storageBackend.deleteObject(globalS3Bucket, s3Key)

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

      const objectMetadata = await storageBackend.headObject(globalS3Bucket, s3Key)
      // update content-length as super user since user may not have update permissions
      const metadata: ObjectMetadata = {
        mimetype: mimeType,
        cacheControl,
        size: objectMetadata.size,
      }
      const { error: updateError, status: updateStatus } = await request.superUserPostgrest
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

      return response.status(uploadResult.httpStatusCode ?? 200).send({
        Key: path,
      })

      /**
       * Remove row from `storage.objects` if there was an error uploading to the backend.
       */
      async function handleUploadError(uploadError: StorageBackendError) {
        request.log.error({ error: uploadError }, 'upload error object')

        // undo operations as super user
        await request.superUserPostgrest
          .from<Obj>('objects')
          .delete()
          .match({
            name: objectName,
            bucket_id: bucketName,
          })
          .single()

        // return an error response
        return response
          .status(uploadError.httpStatusCode)
          .send(
            createResponse(
              uploadError.name,
              String(uploadError.httpStatusCode),
              uploadError.message
            )
          )
      }
    }
  )
}
