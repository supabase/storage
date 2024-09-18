import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { SignedToken, verifyJWT } from '../../../internal/auth'
import { getJwtSecret } from '../../../internal/database'
import { ROUTE_OPERATIONS } from '../operations'
import { ERRORS } from '../../../internal/errors'

const { storageS3Bucket } = getConfig()

const getSignedObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
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
    '/sign/:bucketName/*',
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
      const path = `${request.params.bucketName}/${request.params['*']}`

      if (url !== path) {
        throw ERRORS.InvalidSignature()
      }

      const s3Key = `${request.tenantId}/${url}`
      request.log.info(s3Key)

      const [bucketName, ...objParts] = url.split('/')
      const obj = await request.storage
        .asSuperUser()
        .from(bucketName)
        .findObject(objParts.join('/'), 'id,version')

      return request.storage.renderer('asset').render(request, response, {
        bucket: storageS3Bucket,
        key: s3Key,
        version: obj.version,
        download,
        expires: new Date(exp * 1000).toUTCString(),
        signal: request.signals.disconnect.signal,
      })
    }
  )
}
