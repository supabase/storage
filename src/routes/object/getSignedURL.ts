import { FastifyInstance } from 'fastify'
import { getPostgrestClient, signJWT, transformPostgrestError } from '../../utils'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../utils/generic-routes'

const getSignedURLParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string' },
    '*': { type: 'string' },
  },
  required: ['bucketName', '*'],
} as const
const getSignedURLBodySchema = {
  type: 'object',
  properties: {
    expiresIn: { type: 'number' },
  },
  required: ['expiresIn'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    signedURL: { type: 'string' },
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
  })

  fastify.post<getSignedURLRequestInterface>(
    '/sign/:bucketName/*',
    {
      schema,
    },
    async (request, response) => {
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const postgrest = getPostgrestClient(jwt)

      const { bucketName } = request.params
      const objectName = request.params['*']
      const { expiresIn } = request.body

      const objectResponse = await postgrest
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
      const token = await signJWT({ url: urlToSign }, expiresIn)

      // @todo parse the url properly
      const signedURL = `/object/sign/${urlToSign}?token=${token}`

      return response.status(200).send({ signedURL })
    }
  )
}
