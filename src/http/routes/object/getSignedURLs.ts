import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'

const getSignedURLsParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketName'],
} as const
const getSignedURLsBodySchema = {
  type: 'object',
  properties: {
    expiresIn: { type: 'integer', minimum: 1, examples: [60000] },
    paths: {
      type: 'array',
      items: { type: 'string' },
      minItems: 1,
      examples: [['folder/cat.png', 'folder/morecats.png']],
    },
  },
  required: ['expiresIn', 'paths'],
} as const
const successResponseSchema = {
  type: 'array',
  items: {
    type: 'object',
    properties: {
      error: {
        error: ['string', 'null'],
        examples: ['Either the object does not exist or you do not have access to it'],
      },
      path: {
        type: 'string',
        examples: ['folder/cat.png'],
      },
      signedURL: {
        type: ['string', 'null'],
        examples: [
          '/object/sign/avatars/folder/cat.png?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cmwiOiJhdmF0YXJzL2ZvbGRlci9jYXQucG5nIiwiaWF0IjoxNjE3NzI2MjczLCJleHAiOjE2MTc3MjcyNzN9.s7Gt8ME80iREVxPhH01ZNv8oUn4XtaWsmiQ5csiUHn4',
        ],
      },
    },
    required: ['error', 'path', 'signedURL'],
  },
}
interface getSignedURLsRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getSignedURLsParamsSchema>
  Body: FromSchema<typeof getSignedURLsBodySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Generate presigned urls to retrieve objects'

  const schema = createDefaultSchema(successResponseSchema, {
    body: getSignedURLsBodySchema,
    params: getSignedURLsParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.post<getSignedURLsRequestInterface>(
    '/sign/:bucketName',
    {
      schema,
    },
    async (request, response) => {
      const { bucketName } = request.params
      const { expiresIn, paths } = request.body

      const signedURLs = await request.storage.from(bucketName).signObjectUrls(paths, expiresIn)

      return response.status(200).send(signedURLs)
    }
  )
}
