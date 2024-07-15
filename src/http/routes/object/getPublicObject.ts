import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { ROUTE_OPERATIONS } from '../operations'

const getPublicObjectParamsSchema = {
  type: 'object',
  properties: {
    Bucket: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['Bucket', '*'],
} as const

const getObjectQuerySchema = {
  type: 'object',
  properties: {
    download: { type: 'string', examples: ['filename.jpg', null] },
  },
} as const

interface getObjectRequestInterface {
  Params: FromSchema<typeof getPublicObjectParamsSchema>
  Headers: {
    range?: string
  }
  Querystring: FromSchema<typeof getObjectQuerySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object from a public bucket'
  fastify.get<getObjectRequestInterface>(
    '/public/:Bucket/*',
    {
      // @todo add success response schema here
      exposeHeadRoute: false,
      schema: {
        params: getPublicObjectParamsSchema,
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
      config: {
        operation: { type: ROUTE_OPERATIONS.GET_PUBLIC_OBJECT },
      },
    },
    async (request, response) => {
      const { Bucket } = request.params
      const objectName = request.params['*']
      const { download } = request.query

      const [, obj] = await Promise.all([
        request.storage.asSuperUser().findBucket(Bucket, 'id,public', {
          isPublic: true,
        }),
        request.storage.asSuperUser().from(Bucket).findObject(objectName, 'id,version'),
      ])

      return request.storage.renderer('asset').render(request, response, {
        bucket: Bucket,
        key: objectName,
        version: obj.version,
        download,
      })
    }
  )
}
