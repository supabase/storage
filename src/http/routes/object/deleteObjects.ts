import { DELETE_OBJECTS_LIMIT_DESCRIPTION, enforceDeleteObjectsLimit } from '@storage/limits'
import { objectSchema } from '@storage/schemas/object'
import { FastifyInstance, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema } from '../../routes-helper'
import { AuthenticatedRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const deleteObjectsParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketName'],
} as const
const deleteObjectsBodySchema = {
  type: 'object',
  properties: {
    prefixes: {
      type: 'array',
      items: {
        // A plain name soft-deletes on a versioned bucket (same as
        // deleteObject with no versionId); {name, versionId} permanently
        // deletes that exact version (same as deleteObject with a
        // versionId) - mirrors real S3 DeleteObjects' {Key, VersionId?}.
        oneOf: [
          { type: 'string' },
          {
            type: 'object',
            properties: {
              name: { type: 'string' },
              versionId: { type: 'string' },
            },
            required: ['name', 'versionId'],
            additionalProperties: false,
          },
        ],
      },
      minItems: 1,
      description: DELETE_OBJECTS_LIMIT_DESCRIPTION,
      examples: [['folder/cat.png', 'folder/morecats.png']],
    },
  },
  required: ['prefixes'],
} as const
const successResponseSchema = {
  type: 'array',
  items: objectSchema,
}
interface deleteObjectsInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof deleteObjectsParamsSchema>
  Body: FromSchema<typeof deleteObjectsBodySchema>
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Delete multiple objects'

  const schema = createDefaultSchema(successResponseSchema, {
    body: deleteObjectsBodySchema,
    params: deleteObjectsParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.delete<deleteObjectsInterface>(
    '/:bucketName',
    {
      schema,
      config: {
        operation: ROUTE_OPERATIONS.DELETE_OBJECTS,
        resources: (req: FastifyRequest<deleteObjectsInterface>) => {
          const { prefixes } = req.body
          return prefixes.map(
            (prefix) =>
              `${req.params.bucketName}/${typeof prefix === 'string' ? prefix : prefix.name}`
          )
        },
      },
    },
    async (request, response) => {
      const { bucketName } = request.params
      const prefixes = request.body['prefixes']

      await enforceDeleteObjectsLimit(request.tenantId, prefixes.length)

      const results = await request.storage.from(bucketName).deleteObjects(prefixes, request.owner)

      return response.status(200).send(results)
    }
  )
}
