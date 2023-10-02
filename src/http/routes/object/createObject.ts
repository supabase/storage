import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../generic-routes'

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
    Id: {
      type: 'string',
    },
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
      const { bucketName } = request.params
      const objectName = request.params['*']

      const isUpsert = request.headers['x-upsert'] === 'true'
      const owner = request.owner as string

      const { objectMetadata, path, id } = await request.storage
        .from(bucketName)
        .uploadNewObject(request, {
          objectName,
          owner,
          isUpsert,
        })

      return response.status(objectMetadata?.httpStatusCode ?? 200).send({
        Id: id,
        Key: path,
      })
    }
  )
}
