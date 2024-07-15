import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { SignedToken, verifyJWT } from '@internal/auth'
import { getJwtSecret } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { ROUTE_OPERATIONS } from '../operations'

const getSignedObjectParamsSchema = {
  type: 'object',
  properties: {
    Bucket: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['Bucket', '*'],
} as const

const getSignedObjectQSSchema = {
  type: 'object',
  properties: {
    download: { type: 'string', examples: ['filename.jpg', null] },
    token: {
      type: 'string',
      examples: [
        'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cmwiOiJidWNrZXQyL3B1YmxpYy9zYWRjYXQtdXBsb2FkMjMucG5nIiwiaWF0IjoxNjE3NzI2MjczLCJleHAiOjE2MTc3MjcyNzN9.uBQcXzuvXxfw-9WgzWMBfE_nR3VOgpvfZe032sfLSSk',
      ],
    },
  },
  required: ['token'],
} as const

interface GetSignedObjectRequestInterface {
  Params: FromSchema<typeof getSignedObjectParamsSchema>
  Querystring: FromSchema<typeof getSignedObjectQSSchema>
  Headers: {
    range?: string
  }
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object via a presigned URL'
  fastify.get<GetSignedObjectRequestInterface>(
    '/sign/:Bucket/*',
    {
      // @todo add success response schema here
      schema: {
        params: getSignedObjectParamsSchema,
        querystring: getSignedObjectQSSchema,
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
      config: {
        operation: { type: ROUTE_OPERATIONS.GET_SIGNED_OBJECT },
      },
    },
    async (request, response) => {
      const { token } = request.query
      const { download } = request.query

      let payload: SignedToken
      const { secret: jwtSecret } = await getJwtSecret(request.tenantId)

      try {
        payload = (await verifyJWT(token, jwtSecret)) as SignedToken
      } catch (e) {
        const err = e as Error
        throw ERRORS.InvalidJWT(err)
      }

      const { url, exp } = payload
      const path = `${request.params.Bucket}/${request.params['*']}`

      if (url !== path) {
        throw ERRORS.InvalidSignature()
      }

      const [bucket, ...objParts] = url.split('/')
      const key = objParts.join('/')

      const obj = await request.storage.asSuperUser().from(bucket).findObject(key, 'id,version')

      return request.storage.renderer('asset').render(request, response, {
        bucket: bucket,
        key,
        version: obj.version,
        download,
        expires: new Date(exp * 1000).toUTCString(),
      })
    }
  )
}
