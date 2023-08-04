import { FastifyInstance, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { StorageBackendError } from '../../../storage'

const getPublicObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
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
    '/public/:bucketName/*',
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
        getParentBucketId: (request: FastifyRequest<getObjectRequestInterface>) => {
          return request.params.bucketName
        },
      },
    },
    async (request, response) => {
      const objectName = request.params['*']
      const { download } = request.query

      if (!request.bucket.public) {
        throw new StorageBackendError('not_found', 400, 'Object not found')
      }

      const obj = await request.storage
        .asSuperUser()
        .from(request.bucket)
        .findObject(objectName, 'id,version')

      // send the object from s3
      const s3Key = request.storage.from(request.bucket).computeObjectPath(objectName)

      return request.storage.renderer('asset').render(request, response, {
        key: s3Key,
        version: obj.version,
        download,
      })
    }
  )
}
