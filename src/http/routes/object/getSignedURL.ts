import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ImageRenderer } from '@storage/renderer'
import { transformationOptionsSchema } from '../../schemas/transformations'
import { isImageTransformationEnabled } from '@storage/limits'
import { ROUTE_OPERATIONS } from '../operations'

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
    transform: {
      type: 'object',
      properties: transformationOptionsSchema,
    },
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
      config: {
        operation: { type: ROUTE_OPERATIONS.SIGN_OBJECT_URL },
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const objectName = request.params['*']
      const { expiresIn } = request.body

      const urlPath = request.url.split('?').shift()
      const imageTransformationEnabled = await isImageTransformationEnabled(request.tenantId)

      const transformationOptions = imageTransformationEnabled
        ? {
            transformations: ImageRenderer.applyTransformation(
              request.body.transform || {},
              true
            ).join(','),
            format: request.body.transform?.format || '',
          }
        : undefined

      const signedURL = await request.storage
        .from(bucketName)
        .signObjectUrl(objectName, urlPath as string, expiresIn, transformationOptions)

      return response.status(200).send({ signedURL })
    }
  )
}
