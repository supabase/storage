import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { ROUTE_OPERATIONS } from '../operations'
import fastifyMultipart from '@fastify/multipart'
import { fileUploadFromRequest } from '@storage/uploader'

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

  fastify.register(fastifyMultipart, {
    limits: {
      fields: 10,
      files: 1,
    },
    throwFileSizeLimit: false,
  })

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
      config: {
        operation: { type: ROUTE_OPERATIONS.UPLOAD_SIGN_OBJECT },
      },
    },
    async (request, response) => {
      // Validate sender
      const { token } = request.query
      const { bucketName } = request.params
      const objectName = request.params['*']

      const { owner, upsert } = await request.storage
        .from(bucketName)
        .verifyObjectSignature(token, objectName)

      // Get bucket information once for better error context
      const bucket = await request.storage
        .asSuperUser()
        .findBucket(bucketName, 'id, name, file_size_limit, allowed_mime_types')

      const { objectMetadata, path } = await request.storage
        .asSuperUser()
        .from(bucketName)
        .uploadNewObject({
          file: await fileUploadFromRequest(request, {
            objectName,
            fileSizeLimit: bucket.file_size_limit,
            allowedMimeTypes: bucket.allowed_mime_types || [],
          }),
          objectName,
          owner,
          isUpsert: upsert,
          signal: request.signals.body.signal,
          bucketContext: {
            name: bucket.name,
            fileSizeLimit: bucket.file_size_limit,
          },
        })

      return response.status(objectMetadata?.httpStatusCode ?? 200).send({
        Key: path,
      })
    }
  )
}
