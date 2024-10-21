import { getConfig } from '../../../config'
import { FromSchema } from 'json-schema-to-ts'
import { FastifyInstance } from 'fastify'
import { ImageRenderer } from '@storage/renderer'
import { transformationOptionsSchema } from '../../schemas/transformations'
import { ROUTE_OPERATIONS } from '../operations'
import { getTenantConfig } from '@internal/database'

const { storageS3Bucket, isMultitenant } = getConfig()

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
    ...transformationOptionsSchema,
    download: { type: 'string', examples: ['filename.png'] },
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
        tags: ['transformation'],
      },
      config: {
        operation: { type: ROUTE_OPERATIONS.RENDER_SIGNED_IMAGE },
      },
    },
    async (request, response) => {
      const { download } = request.query
      const { bucketName } = request.params
      const objectName = request.params['*']

      const obj = await request.storage.from(bucketName).findObject(objectName, 'id,version')

      const s3Key = `${request.tenantId}/${bucketName}/${objectName}`

      const renderer = request.storage.renderer('image') as ImageRenderer

      if (isMultitenant) {
        const tenantConfig = await getTenantConfig(request.tenantId)
        renderer.setLimits({
          maxResolution: tenantConfig.features.imageTransformation.maxResolution,
        })
      }

      return renderer.setTransformations(request.query).render(request, response, {
        bucket: storageS3Bucket,
        key: s3Key,
        version: obj.version,
        download,
        signal: request.signals.disconnect.signal,
      })
    }
  )
}
