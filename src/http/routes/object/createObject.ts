import fastifyMultipart from '@fastify/multipart'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../routes-helper'
import { ROUTE_OPERATIONS } from '../operations'

const createObjectParamsSchema = {
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
    Id: {
      type: 'string',
    },
    Key: {
      type: 'string',
      examples: ['avatars/folder/cat.png'],
    },
    VersionId: {
      type: 'string',
      examples: ['eaa8bdb5-2e00-4767-b5a9-d2502efe2196', 'null'],
    },
  },
  required: ['Key'],
}
interface createObjectRequestInterface extends RequestGenericInterface {
  Params: FromSchema<typeof createObjectParamsSchema>
  Headers: {
    authorization: string
    'content-type': string
    'cache-control'?: string
    'x-upsert'?: string
    'x-robots-tag'?: string
  }
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Upload a new object'

  const schema = createDefaultSchema(successResponseSchema, {
    params: createObjectParamsSchema,
    summary,
    tags: ['object'],
  })

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

  fastify.post<createObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema,
      config: {
        operation: ROUTE_OPERATIONS.CREATE_OBJECT,
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const objectName = request.params['*']

      const isUpsert = request.headers['x-upsert'] === 'true'
      const owner = request.owner

      const { objectMetadata, path, id, versionId } = await request.storage
        .from(bucketName)
        .uploadFromRequest(request, {
          objectName,
          signal: request.signals.body.signal,
          owner,
          isUpsert,
        })

      return response.status(objectMetadata?.httpStatusCode ?? 200).send({
        Id: id,
        Key: path,
        VersionId: versionId,
      })
    }
  )
}
