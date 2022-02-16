import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { getJwtSecret, signJWT, transformPostgrestError } from '../../utils'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'

const getSignedURLsParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', example: 'avatars' },
  },
  required: ['bucketName'],
} as const
const getSignedURLsBodySchema = {
  type: 'object',
  properties: {
    expiresIn: { type: 'integer', minimum: 1, example: 60000 },
    prefixes: {
      type: 'array',
      items: { type: 'string' },
      minItems: 1,
      maxItems: 1000,
      example: ['folder/cat.png', 'folder/morecats.png'],
    },
  },
  required: ['expiresIn', 'prefixes'],
} as const
const successResponseSchema = {
  type: 'array',
  items: {
    type: 'object',
    properties: {
      prefix: {
        type: 'string',
        example: 'folder/cat.png',
      },
      signedURL: {
        type: 'string',
        example:
          '/object/sign/avatars/folder/cat.png?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cmwiOiJhdmF0YXJzL2ZvbGRlci9jYXQucG5nIiwiaWF0IjoxNjE3NzI2MjczLCJleHAiOjE2MTc3MjcyNzN9.s7Gt8ME80iREVxPhH01ZNv8oUn4XtaWsmiQ5csiUHn4',
      },
    },
    required: ['signedURL'],
  },
}
interface getSignedURLsRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getSignedURLsParamsSchema>
  Body: FromSchema<typeof getSignedURLsBodySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
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
      const { expiresIn, prefixes } = request.body

      const objectResponse = await request.postgrest
        .from<Obj>('objects')
        .select('name')
        .eq('bucket_id', bucketName)
        .in('name', prefixes)

      if (objectResponse.error) {
        const { error, status } = objectResponse
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
      }

      const { data: results } = objectResponse
      request.log.info({ results }, 'results')

      const nameSet = new Set(results.map(({ name }) => name))
      const difference = [...new Set(prefixes)].filter((prefix) => !nameSet.has(prefix))

      if (difference.length > 0) {
        return response
          .status(400)
          .send(
            createResponse(
              `Either the objects do not exist or you do not have access to: ${difference.join(
                ' ,'
              )}`,
              '400',
              'Non-existent or no access'
            )
          )
      }

      const jwtSecret = await getJwtSecret(request.tenantId)
      const signedUrls = await Promise.all(
        prefixes.map(async (prefix) => {
          const urlToSign = `${bucketName}/${prefix}`
          request.log.info(`going to sign ${urlToSign}`)
          const token = await signJWT({ url: urlToSign }, jwtSecret, expiresIn)
          return {
            prefix,
            signedURL: `/object/sign/${urlToSign}?token=${token}`,
          }
        })
      )

      return response.status(200).send(signedUrls)
    }
  )
}
