import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { getJwtSecret, signJWT, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { createDefaultSchema } from '../../utils/generic-routes'

const { urlLengthLimit } = getConfig()

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
      const { expiresIn, paths } = request.body
      let results: { name: string }[] = []

      for (let i = 0; i < paths.length; ) {
        const pathsSubset = []
        let urlParamLength = 0

        for (; i < paths.length && urlParamLength < urlLengthLimit; i++) {
          const path = paths[i]
          pathsSubset.push(path)
          urlParamLength += encodeURIComponent(path).length + 9 // length of '%22%2C%22'
        }

        const objectResponse = await request.postgrest
          .from<Obj>('objects')
          .select('name')
          .eq('bucket_id', bucketName)
          .in('name', pathsSubset)

        if (objectResponse.error) {
          const { error, status } = objectResponse
          request.log.error({ error }, 'failed to retrieve object names while getting signed URLs')
          return response.status(400).send(transformPostgrestError(error, status))
        }

        const { data } = objectResponse
        results = results.concat(data)
      }

      const nameSet = new Set(results.map(({ name }) => name))

      const jwtSecret = await getJwtSecret(request.tenantId)
      const signedURLs = await Promise.all(
        paths.map(async (path) => {
          let error = null
          let signedURL = null
          if (nameSet.has(path)) {
            const urlToSign = `${bucketName}/${path}`
            const token = await signJWT({ url: urlToSign }, jwtSecret, expiresIn)
            signedURL = `/object/sign/${urlToSign}?token=${token}`
          } else {
            error = 'Either the object does not exist or you do not have access to it'
          }
          return {
            error,
            path,
            signedURL,
          }
        })
      )

      return response.status(200).send(signedURLs)
    }
  )
}
