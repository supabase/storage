import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'
import { ImageRenderer } from '../../../storage/renderer'
import { transformationQueryString } from '../../schemas/transformations'

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
const renderImageQuerySchema = {
  type: 'object',
  properties: {
    ...transformationQueryString,
  },
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
  Querystring: FromSchema<typeof renderImageQuerySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Generate a presigned url to retrieve an object'

  const schema = createDefaultSchema(successResponseSchema, {
    body: getSignedURLBodySchema,
    params: getSignedURLParamsSchema,
    querystring: renderImageQuerySchema,
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

      const urlPath = request.url.split('?').shift()

      const signedURL = await request.storage
        .from(bucketName)
        .signObjectUrl(objectName, urlPath as string, expiresIn, {
          transformations: ImageRenderer.applyTransformation(request.query || {}).join(','),
        })

      return response.status(200).send({ signedURL })
    }
  )
}
