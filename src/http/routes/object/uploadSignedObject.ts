import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getJwtSecret, SignedUploadToken, verifyJWT } from '../../../auth'
import { StorageBackendError } from '../../../storage'

const uploadSignedObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
} as const

const uploadSignedObjectQSSchema = {
  type: 'object',
  properties: {
    token: {
      type: 'string',
      examples: [
        'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cmwiOiJidWNrZXQyL3B1YmxpYy9zYWRjYXQtdXBsb2FkMjMucG5nIiwiaWF0IjoxNjE3NzI2MjczLCJleHAiOjE2MTc3MjcyNzN9.uBQcXzuvXxfw-9WgzWMBfE_nR3VOgpvfZe032sfLSSk',
      ],
    },
  },
  required: ['token'],
} as const

const successResponseSchema = {
  type: 'object',
  properties: {
    Key: { type: 'string', examples: ['avatars/folder/cat.png'] },
  },
  required: ['Key'],
}

interface UploadSignedObjectRequestInterface {
  Params: FromSchema<typeof uploadSignedObjectParamsSchema>
  Querystring: FromSchema<typeof uploadSignedObjectQSSchema>
  Headers: {
    range?: string
  }
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Uploads an object via a presigned URL'

  fastify.addContentTypeParser(
    ['application/json', 'text/plain'],
    function (request, payload, done) {
      done(null)
    }
  )

  fastify.put<UploadSignedObjectRequestInterface>(
    '/upload/sign/:bucketName/*',
    {
      // @todo add success response schema here
      schema: {
        params: uploadSignedObjectParamsSchema,
        querystring: uploadSignedObjectQSSchema,
        summary,
        response: {
          200: { description: 'Successful response', ...successResponseSchema },
          '4xx': { $ref: 'errorSchema#', description: 'Error response' },
        },
        tags: ['object'],
      },
    },
    async (request, response) => {
      // Validate sender
      const { token } = request.query

      const jwtSecret = await getJwtSecret(request.tenantId)

      let payload: SignedUploadToken
      try {
        payload = (await verifyJWT(token, jwtSecret)) as SignedUploadToken
      } catch (e) {
        const err = e as Error
        throw new StorageBackendError('Invalid JWT', 400, err.message, err)
      }

      const { url, exp, owner } = payload
      const { bucketName } = request.params
      const objectName = request.params['*']

      if (url !== `${bucketName}/${objectName}`) {
        throw new StorageBackendError('InvalidSignature', 400, 'The url do not match the signature')
      }

      if (exp * 1000 < Date.now()) {
        throw new StorageBackendError('ExpiredSignature', 400, 'The signature has expired')
      }

      const { objectMetadata, path } = await request.storage
        .asSuperUser()
        .from(bucketName)
        .uploadNewObject(request, {
          owner,
          objectName,
        })

      return response.status(objectMetadata?.httpStatusCode ?? 200).send({
        Key: path,
      })
    }
  )
}
