import { FromSchema } from 'json-schema-to-ts'
import { FastifyInstance, FastifyRequest } from 'fastify'
import { ImageRenderer } from '../../../storage/renderer'
import { getJwtSecret, SignedToken, verifyJWT } from '../../../auth'
import { StorageBackendError } from '../../../storage'

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
        tags: ['object'],
      },
      config: {
        getParentBucketId: (request: FastifyRequest<renderImageRequestInterface>) => {
          return request.params.bucketName
        },
      },
    },
    async (request, response) => {
      const { token } = request.query
      const { download } = request.query

      let payload: SignedToken
      const jwtSecret = await getJwtSecret(request.tenantId)

      try {
        payload = (await verifyJWT(token, jwtSecret)) as SignedToken
      } catch (e) {
        const err = e as Error
        throw new StorageBackendError('Invalid JWT', 400, err.message, err)
      }

      const { url, transformations, exp } = payload

      const path = `${request.params.bucketName}/${request.params['*']}`

      if (url !== path) {
        throw new StorageBackendError('InvalidSignature', 400, 'The url do not match the signature')
      }

      const objectName = request.params['*']
      const obj = await request.storage
        .asSuperUser()
        .from(request.bucket)
        .findObject(objectName, 'id,version')

      const s3Key = request.storage.from(request.bucket).computeObjectPath(obj.name)

      const renderer = request.storage.renderer('image') as ImageRenderer
      return renderer
        .setTransformationsFromString(transformations || '')
        .render(request, response, {
          key: s3Key,
          version: obj.version,
          download,
          expires: new Date(exp * 1000).toUTCString(),
        })
    }
  )
}
