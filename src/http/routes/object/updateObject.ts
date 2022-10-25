import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../generic-routes'
import { ObjectMetadata } from '../../../storage/backend'

const updateObjectParamsSchema = {
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
    Key: { type: 'string', examples: ['projectref/avatars/folder/cat.png'] },
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
      const contentType = request.headers['content-type']
      request.log.info(`content-type is ${contentType}`)

      const { bucketName } = request.params
      const objectName = request.params['*']
      const path = `${bucketName}/${objectName}`
      const s3Key = `${request.tenantId}/${path}`
      const owner = request.owner

      await request.storage.from(bucketName).updateObjectOwner(objectName, owner)

      const { error, isTruncated, objectMetadata } = await request.storage
        .uploader()
        .upload(request, {
          key: s3Key,
        })

      if (error) {
        return response
          .status(error.httpStatusCode)
          .send(createResponse(error.name, String(error.httpStatusCode), error.message))
      }

      if (isTruncated) {
        // @todo tricky to handle since we need to undo the s3 upload
      }

      await request.storage
        .from(bucketName)
        .updateObjectMetadata(objectName, objectMetadata as ObjectMetadata)

      return response.status(objectMetadata?.httpStatusCode ?? 200).send({
        Key: path,
      })
    }
  )
}
