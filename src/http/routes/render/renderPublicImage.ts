import { FromSchema } from 'json-schema-to-ts'
import { FastifyInstance, FastifyRequest } from 'fastify'
import { ImageRenderer } from '../../../storage/renderer'
import { transformationOptionsSchema } from '../../schemas/transformations'
import { StorageBackendError } from '../../../storage'

const renderPublicImageParamsSchema = {
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
    ...transformationOptionsSchema,
    download: { type: 'string', examples: ['filename.png'] },
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
      config: {
        getParentBucketId: (request: FastifyRequest<renderImageRequestInterface>) => {
          return request.params.bucketName
        },
      },
    },
    async (request, response) => {
      const { download } = request.query
      const objectName = request.params['*']

      if (!request.bucket.public) {
        throw new StorageBackendError('not_found', 400, 'Object not found')
      }

      const obj = await request.storage
        .asSuperUser()
        .from(request.bucket)
        .findObject(objectName, 'id,version')

      const s3Key = request.storage.from(request.bucket).computeObjectPath(objectName)

      const renderer = request.storage.renderer('image') as ImageRenderer

      return renderer.setTransformations(request.query).render(request, response, {
        key: s3Key,
        version: obj.version,
        download,
      })
    }
  )
}
