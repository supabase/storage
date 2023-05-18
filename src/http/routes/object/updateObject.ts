import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../generic-routes'

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
    Id: {
      type: 'string',
    },
    Key: { type: 'string', examples: ['avatars/folder/cat.png'] },
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
      const owner = request.owner as string

      const { objectMetadata, path, id } = await request.storage
        .from(bucketName)
        .uploadOverridingObject(request, {
          owner,
          objectName: objectName,
        })

      return response.status(objectMetadata?.httpStatusCode ?? 200).send({
        Id: id,
        Key: path,
      })
    }
  )
}
