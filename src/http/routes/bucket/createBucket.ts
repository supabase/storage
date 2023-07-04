import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../generic-routes'
import { AuthenticatedRequest } from '../../request'
import { mustBeServiceKey } from '../../../auth'

const createBucketBodySchema = {
  type: 'object',
  properties: {
    name: { type: 'string', examples: ['avatars'] },
    id: { type: 'string', examples: ['avatars'] },
    public: { type: 'boolean', examples: [false] },
    credential_id: { type: 'string' },
    file_size_limit: {
      anyOf: [
        { type: 'integer', examples: [1000], nullable: true, minimum: 0 },
        { type: 'string', examples: ['100MB'], nullable: true },
      ],
    },
    allowed_mime_types: {
      type: 'array',
      nullable: true,
      examples: [['image/png', 'image/jpg']],
      items: { type: 'string' },
    },
  },
  required: ['name'],
} as const

const successResponseSchema = {
  type: 'object',
  properties: {
    name: { type: 'string', examples: ['avatars'] },
  },
  required: ['name'],
}
interface createBucketRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof createBucketBodySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Create a bucket'
  const schema = createDefaultSchema(successResponseSchema, {
    allowUnionTypes: true,
    body: createBucketBodySchema,
    summary,
    tags: ['bucket'],
  })
  fastify.post<createBucketRequestInterface>(
    '/',
    {
      schema,
    },
    async (request, response) => {
      const owner = request.owner

      const {
        name: bucketName,
        public: isPublic,
        id,
        allowed_mime_types,
        file_size_limit,
        credential_id,
      } = request.body

      if (credential_id) {
        await mustBeServiceKey(request.tenantId, request.jwt)
      }

      const bucket = await request.storage.createBucket({
        id: id ?? bucketName,
        name: bucketName,
        owner,
        public: isPublic ?? false,
        fileSizeLimit: file_size_limit,
        credentialId: credential_id,
        allowedMimeTypes: allowed_mime_types
          ? allowed_mime_types?.filter((mime) => mime)
          : allowed_mime_types,
      })

      request.log.info({ results: bucket }, 'results')

      return response.status(200).send({
        name: bucketName,
      })
    }
  )
}
