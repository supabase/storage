import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'
import { getConfig } from '../../../config'

const { signedUploadUrlExpirationTime } = getConfig()

const getSignedUploadURLParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
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
  },
  required: ['url'],
}
interface getSignedURLRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getSignedUploadURLParamsSchema>
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
    },
    async (request, response) => {
      const { bucketName } = request.params
      const objectName = request.params['*']
      const owner = request.owner

      const urlPath = request.url.split('?').shift()

      const signedUploadURL = await request.storage
        .from(bucketName)
        .signUploadObjectUrl(objectName, urlPath as string, signedUploadUrlExpirationTime, owner)

      return response.status(200).send({ url: signedUploadURL })
    }
  )
}
