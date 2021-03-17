import { FastifyInstance } from 'fastify'
import { getPostgrestClient, signJWT, transformPostgrestError } from '../../utils'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

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
  fastify.post<getSignedURLRequestInterface>(
    '/sign/:bucketName/*',
    {
      schema: {
        body: getSignedURLBodySchema,
        params: getSignedURLParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { 200: successResponseSchema, '4xx': { $ref: 'errorSchema#' } },
      },
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
        .select('*, buckets(*)')
        .match({
          name: objectName,
          'buckets.name': bucketName,
        })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        console.log(error)
        return response.status(400).send(transformPostgrestError(error, status))
      }
      const { data: results } = objectResponse
      console.log(results)

      if (!results.buckets) {
        // @todo why is this check necessary?
        return response.status(400).send({
          statusCode: '404',
          error: 'Not found',
          message: 'The requested bucket was not found',
        })
      }

      console.log(`going to sign ${request.url}`)
      const urlParts = request.url.split('/')
      const urlToSign = decodeURI(urlParts.splice(3).join('/'))
      const token = await signJWT({ url: urlToSign }, expiresIn)

      // @todo parse the url properly
      const signedURL = `/object/sign/${urlToSign}?token=${token}`

      return response.status(200).send({ signedURL })
    }
  )
}
