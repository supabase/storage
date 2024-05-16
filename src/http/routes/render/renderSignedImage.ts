import { FromSchema } from 'json-schema-to-ts'
import { FastifyInstance } from 'fastify'
import { getConfig } from '../../../config'
import { ImageRenderer } from '../../../storage/renderer'
import { SignedToken, verifyJWT } from '../../../auth'
import { ERRORS } from '../../../storage'
import { getJwtSecret } from '../../../database/tenant'
import { ROUTE_OPERATIONS } from '../operations'

const { storageS3Bucket } = getConfig()

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
    token: {
      type: 'string',
      examples: [
        'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cmwiOiJidWNrZXQyL3B1YmxpYy9zYWRjYXQtdXBsb2FkMjMucG5nIiwiaWF0IjoxNjE3NzI2MjczLCJleHAiOjE2MTc3MjcyNzN9.uBQcXzuvXxfw-9WgzWMBfE_nR3VOgpvfZe032sfLSSk',
      ],
    },
    download: { type: 'string', examples: ['filename.png'] },
  },
  required: ['token'],
} as const

interface renderImageRequestInterface {
  Params: FromSchema<typeof renderAuthenticatedImageParamsSchema>
  Querystring: FromSchema<typeof renderImageQuerySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Render an authenticated image with the given transformations'
  fastify.get<renderImageRequestInterface>(
    '/sign/:bucketName/*',
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
      const { token } = request.query
      const { download } = request.query

      let payload: SignedToken
      const { secret: jwtSecret } = await getJwtSecret(request.tenantId)

      try {
        payload = (await verifyJWT(token, jwtSecret)) as SignedToken
      } catch (e) {
        const err = e as Error
        throw ERRORS.InvalidJWT(err)
      }

      const { url, transformations, exp } = payload

      const path = `${request.params.bucketName}/${request.params['*']}`

      if (url !== path) {
        throw ERRORS.InvalidSignature()
      }

      const s3Key = `${request.tenantId}/${url}`

      const [bucketName, ...objParts] = url.split('/')
      const obj = await request.storage
        .asSuperUser()
        .from(bucketName)
        .findObject(objParts.join('/'), 'id,version')

      const renderer = request.storage.renderer('image') as ImageRenderer
      return renderer
        .setTransformationsFromString(transformations || '')
        .render(request, response, {
          bucket: storageS3Bucket,
          key: s3Key,
          version: obj.version,
          download,
          expires: new Date(exp * 1000).toUTCString(),
        })
    }
  )
}
