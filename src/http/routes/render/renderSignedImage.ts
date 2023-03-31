import { getConfig } from '../../../config'
import { FromSchema } from 'json-schema-to-ts'
import { FastifyInstance } from 'fastify'
import { ImageRenderer } from '../../../storage/renderer'
import { getJwtSecret, SignedToken, verifyJWT } from '../../../auth'
import { StorageBackendError } from '../../../storage'

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
    token: { type: 'string' },
    download: { type: 'string' },
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

      const s3Key = `${request.tenantId}/${url}`
      request.log.info(s3Key)

      const [bucketName, ...objParts] = url.split('/')
      const obj = await request.storage
        .asSuperUser()
        .from(bucketName)
        .findObject(objParts.join('/'), 'id,version')

      const renderer = request.storage.renderer('image') as ImageRenderer
      return renderer
        .setTransformationsFromString(transformations || '')
        .render(request, response, {
          bucket: globalS3Bucket,
          key: s3Key,
          version: obj.version,
          download,
          expires: new Date(exp * 1000).toUTCString(),
        })
    }
  )
}
