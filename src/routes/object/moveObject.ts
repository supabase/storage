import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { getPostgrestClient, isValidKey, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'
import { S3Backend } from '../../backend/s3'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const storageBackend = new S3Backend(region, globalS3Endpoint)

const moveObjectsBodySchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', example: 'avatars' },
    sourceKey: { type: 'string', example: 'folder/cat.png' },
    destinationKey: { type: 'string', example: 'folder/newcat.png' },
  },
  required: ['bucketId', 'sourceKey', 'destinationKey'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string', example: 'Successfully moved' },
  },
  required: ['message'],
}
interface moveObjectRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof moveObjectsBodySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Moves an object'

  const schema = createDefaultSchema(successResponseSchema, {
    body: moveObjectsBodySchema,
    summary,
    tags: ['object'],
  })

  fastify.post<moveObjectRequestInterface>(
    '/move',
    {
      schema,
    },
    async (request, response) => {
      // check if the user is able to update the row
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const { destinationKey, sourceKey, bucketId } = request.body

      if (!isValidKey(destinationKey)) {
        return response
          .status(400)
          .send(
            createResponse('The destination key contains invalid characters', '400', 'Invalid key')
          )
      }

      const postgrest = getPostgrestClient(jwt)

      const objectResponse = await postgrest
        .from<Obj>('objects')
        .update({
          last_accessed_at: new Date().toISOString(),
          name: destinationKey,
        })
        .match({ bucket_id: bucketId, name: sourceKey })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
      }

      // if successfully updated, copy and delete object from s3
      const oldS3Key = `${projectRef}/${bucketId}/${sourceKey}`
      const newS3Key = `${projectRef}/${bucketId}/${destinationKey}`

      // @todo what happens if one of these fail?
      await storageBackend.copyObject(globalS3Bucket, oldS3Key, newS3Key)
      await storageBackend.deleteObject(globalS3Bucket, oldS3Key)

      return response.status(200).send(createResponse('Successfully moved'))
    }
  )
}
