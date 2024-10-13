import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { ROUTE_OPERATIONS } from '../operations'

const { storageS3Bucket } = getConfig()

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
        operation: { type: ROUTE_OPERATIONS.GET_PUBLIC_OBJECT },
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const objectName = request.params['*']
      const { download } = request.query

      const [, obj] = await Promise.all([
        request.storage.asSuperUser().findBucket(bucketName, 'id,public', {
          isPublic: true,
        }),
        request.storage.asSuperUser().from(bucketName).findObject(objectName, 'id,version'),
      ])

      // send the object from s3
      const s3Key = `${request.tenantId}/${bucketName}/${objectName}`

      return request.storage.renderer('asset').render(request, response, {
        bucket: storageS3Bucket,
        key: s3Key,
        version: obj.version,
        download,
        signal: request.signals.disconnect.signal,
      })
    }
  )
}
