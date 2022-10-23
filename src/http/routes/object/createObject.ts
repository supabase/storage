import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../generic-routes'
import { ObjectMetadata } from '../../../storage/backend'

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

      const isUpsert = request.headers['x-upsert'] === 'true'

      const object = await request.storage.from(bucketName).createObject(
        {
          name: objectName,
          owner: request.owner,
        },
        isUpsert
      )

      request.log.info({ results: object }, 'results')

      const { error, isTruncated, objectMetadata } = await request.storage
        .uploader()
        .upload(request, {
          key: s3Key,
        })

      if (error || isTruncated) {
        // undo operations as super user
        await request.storage.asSuperUser().from(bucketName).deleteObject(objectName)
      }

      if (error) {
        return response
          .status(error.httpStatusCode)
          .send(createResponse(error.name, String(error.httpStatusCode), error.message))
      }

      if (isTruncated) {
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

      await request.storage
        .asSuperUser()
        .from(bucketName)
        .updateObjectMetadata(objectName, objectMetadata as ObjectMetadata)

      return response.status(objectMetadata?.httpStatusCode ?? 200).send({
        Key: path,
      })
    }
  )
}
