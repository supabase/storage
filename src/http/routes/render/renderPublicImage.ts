import { getConfig } from '../../../config'
import { FromSchema } from 'json-schema-to-ts'
import { FastifyInstance } from 'fastify'
import { ImageRenderer } from '../../../storage/renderer'
import { transformationQueryString } from '../../schemas/transformations'

const { globalS3Bucket } = getConfig()

const renderPublicImageParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    download: { type: 'string' },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
} as const

const renderImageQuerySchema = {
  type: 'object',
  properties: {
    ...transformationQueryString,
    download: { type: 'string' },
  },
} as const

interface renderImageRequestInterface {
  Params: FromSchema<typeof renderPublicImageParamsSchema>
  Querystring: FromSchema<typeof renderImageQuerySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Render a public image with the given transformations'
  fastify.get<renderImageRequestInterface>(
    '/public/:bucketName/*',
    {
      schema: {
        params: renderPublicImageParamsSchema,
        querystring: renderImageQuerySchema,
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      const { download } = request.query
      const { bucketName } = request.params
      const objectName = request.params['*']

      await request.storage.asSuperUser().from(bucketName).findObject(objectName)

      const s3Key = `${request.tenantId}/${bucketName}/${objectName}`

      const renderer = request.storage.renderer('image') as ImageRenderer

      return renderer.setTransformations(request.query).render(request, response, {
        bucket: globalS3Bucket,
        key: s3Key,
        download,
      })
    }
  )
}
