import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { getConfig } from '../../../config'
import { ROUTE_OPERATIONS } from '../operations'

const { uploadSignedUrlExpirationTime } = getConfig()

const getSignedUploadURLParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
} as const

const getSignedUploadURLHeadersSchema = {
  type: 'object',
  properties: {
    'x-upsert': { type: 'string' },
    authorization: { type: 'string' },
  },
  required: ['authorization'],
} as const

const successResponseSchema = {
  type: 'object',
  properties: {
    url: {
      type: 'string',
      examples: [
        '/object/sign/upload/avatars/folder/cat.png?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cmwiOiJhdmF0YXJzL2ZvbGRlci9jYXQucG5nIiwiaWF0IjoxNjE3NzI2MjczLCJleHAiOjE2MTc3MjcyNzN9.s7Gt8ME80iREVxPhH01ZNv8oUn4XtaWsmiQ5csiUHn4',
      ],
    },
    token: {
      type: 'string',
    },
  },
  required: ['url'],
}
interface getSignedURLRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getSignedUploadURLParamsSchema>
  Headers: FromSchema<typeof getSignedUploadURLHeadersSchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Generate a presigned url to upload an object'

  const schema = createDefaultSchema(successResponseSchema, {
    params: getSignedUploadURLParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.post<getSignedURLRequestInterface>(
    '/upload/sign/:bucketName/*',
    {
      schema,
      config: {
        operation: { type: ROUTE_OPERATIONS.SIGN_UPLOAD_URL },
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const objectName = request.params['*']
      const owner = request.owner

      const urlPath = `${bucketName}/${objectName}`

      const signedUpload = await request.storage
        .from(bucketName)
        .signUploadObjectUrl(objectName, urlPath as string, uploadSignedUrlExpirationTime, owner, {
          upsert: request.headers['x-upsert'] === 'true',
        })

      return response.status(200).send({ url: signedUpload.url, token: signedUpload.token })
    }
  )
}
