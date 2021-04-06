import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { objectSchema } from '../../schemas/object'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { getPostgrestClient, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { createDefaultSchema } from '../../utils/generic-routes'
import { deleteObjects, initClient } from '../../utils/s3'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const deleteObjectsParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', example: 'avatars' },
  },
  required: ['bucketName'],
} as const
const deleteObjectsBodySchema = {
  type: 'object',
  properties: {
    prefixes: {
      type: 'array',
      items: { type: 'string' },
      minItems: 1,
      maxItems: 1000,
      example: ['folder/cat.png', 'folder/morecats.png'],
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

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
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
    },
    async (request, response) => {
      // check if the user is able to insert that row
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const { bucketName } = request.params
      const prefixes = request.body['prefixes']

      const postgrest = getPostgrestClient(jwt)

      const objectResponse = await postgrest
        .from<Obj>('objects')
        .delete()
        .eq('bucket_id', bucketName)
        .in('name', prefixes)

      if (objectResponse.error) {
        const { error, status } = objectResponse
        request.log.error({ error }, 'error object')
        return response.status(status).send(transformPostgrestError(error, status))
      }

      const { data: results } = objectResponse
      if (results.length > 0) {
        // if successfully deleted, delete from s3 too
        const prefixesToDelete = results.map((ele) => {
          return { Key: `${projectRef}/${bucketName}/${ele.name}` }
        })

        await deleteObjects(client, globalS3Bucket, prefixesToDelete)
      }

      return response.status(200).send(results)
    }
  )
}
