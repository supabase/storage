import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { getJwtSecret, signJWT, transformPostgrestError } from '../../utils'
import { createDefaultSchema } from '../../utils/generic-routes'

const getSignedURLParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
} as const
const getSignedURLBodySchema = {
  type: 'object',
  properties: {
    expiresIn: { type: 'integer', minimum: 1, examples: [60000] },
  },
  required: ['expiresIn'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    signedURL: {
      type: 'string',
      examples: [
        '/object/sign/avatars/folder/cat.png?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cmwiOiJhdmF0YXJzL2ZvbGRlci9jYXQucG5nIiwiaWF0IjoxNjE3NzI2MjczLCJleHAiOjE2MTc3MjcyNzN9.s7Gt8ME80iREVxPhH01ZNv8oUn4XtaWsmiQ5csiUHn4',
      ],
    },
  },
  required: ['signedURL'],
}
interface getSignedURLRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getSignedURLParamsSchema>
  Body: FromSchema<typeof getSignedURLBodySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Generate a presigned url to retrieve an object'

  const schema = createDefaultSchema(successResponseSchema, {
    body: getSignedURLBodySchema,
    params: getSignedURLParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.post<getSignedURLRequestInterface>(
    '/sign/:bucketName/*',
    {
      schema,
    },
    async (request, response) => {
      const { bucketName } = request.params
      const objectName = request.params['*']
      const { expiresIn } = request.body

      const objectResponse = await request.postgrest
        .from<Obj>('objects')
        .select('id')
        .match({
          name: objectName,
          bucket_id: bucketName,
        })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      const { data: results } = objectResponse
      request.log.info({ results }, 'results')

      request.log.info(`going to sign ${request.url}`)
      const urlParts = request.url.split('/')
      const urlToSign = decodeURI(urlParts.splice(3).join('/'))
      const jwtSecret = await getJwtSecret(request.tenantId)
      const token = await signJWT({ url: urlToSign }, jwtSecret, expiresIn)

      // @todo parse the url properly
      const signedURL = `/object/sign/${urlToSign}?token=${token}`

      return response.status(200).send({ signedURL })
    }
  )
}
