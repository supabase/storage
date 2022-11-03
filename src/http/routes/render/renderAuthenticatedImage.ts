import { getConfig } from '../../../config'
import { FromSchema } from 'json-schema-to-ts'
import { FastifyInstance } from 'fastify'
import { ImageRenderer } from '../../../storage/renderer'

const { globalS3Bucket } = getConfig()

const renderAuthenticatedImageParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
} as const

const renderImageQuerySchema = {
  type: 'object',
  properties: {
    height: { type: 'integer', examples: [100], minimum: 0 },
    width: { type: 'integer', examples: [100], minimum: 0 },
    resize: { type: 'string', enum: ['fill', 'fit', 'fill-down', 'force', 'auto'] },
  },
} as const

interface renderImageRequestInterface {
  Params: FromSchema<typeof renderAuthenticatedImageParamsSchema>
  Querystring: FromSchema<typeof renderImageQuerySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Render an authenticated image with the given transformations'
  fastify.get<renderImageRequestInterface>(
    '/authenticated/:bucketName/*',
    {
      schema: {
        params: renderAuthenticatedImageParamsSchema,
        querystring: renderImageQuerySchema,
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const objectName = request.params['*']

      await request.storage.from(bucketName).findObject(objectName)

      const s3Key = `${request.tenantId}/${bucketName}/${objectName}`

      const renderer = request.storage.renderer('image') as ImageRenderer

      return renderer.setTransformations(request.query).render(request, response, {
        bucket: globalS3Bucket,
        key: s3Key,
      })
    }
  )
}
