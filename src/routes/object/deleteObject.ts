import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { getPostgrestClient, isValidKey, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'
import { deleteObject, initClient } from '../../utils/s3'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const deleteObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string' },
    '*': { type: 'string' },
  },
  required: ['bucketName', '*'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string' },
  },
}
interface deleteObjectRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof deleteObjectParamsSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Delete an object'

  const schema = createDefaultSchema(successResponseSchema, {
    params: deleteObjectParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.delete<deleteObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema,
    },
    async (request, response) => {
      // check if the user is able to insert that row
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const { bucketName } = request.params
      const objectName = request.params['*']

      const postgrest = getPostgrestClient(jwt)

      if (!isValidKey(objectName) || !isValidKey(bucketName)) {
        return response
          .status(400)
          .send(createResponse('The key contains invalid characters', '400', 'Invalid key'))
      }

      const objectResponse = await postgrest
        .from<Obj>('objects')
        .delete()
        .match({
          name: objectName,
          bucket_id: bucketName,
        })
        .single()

      if (objectResponse.error) {
        const { error, status } = objectResponse
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      const { data: results } = objectResponse
      request.log.info({ results }, 'results')

      // if successfully deleted, delete from s3 too
      const s3Key = `${projectRef}/${bucketName}/${objectName}`
      await deleteObject(client, globalS3Bucket, s3Key)

      return response.status(200).send(createResponse('Successfully deleted'))
    }
  )
}
