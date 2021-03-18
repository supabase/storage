import { FastifyInstance } from 'fastify'
import { getPostgrestClient, isValidKey, transformPostgrestError } from '../../utils'
import { initClient, copyObject, deleteObject } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { Obj, AuthenticatedRequest } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const renameObjectsBodySchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string' },
    sourceKey: { type: 'string' },
    destinationKey: { type: 'string' },
  },
  required: ['bucketName', 'sourceKey', 'destinationKey'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string' },
  },
  required: ['message'],
}
interface renameObjectRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof renameObjectsBodySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Rename an object'
  fastify.post<renameObjectRequestInterface>(
    '/rename',
    {
      schema: {
        body: renameObjectsBodySchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { 200: successResponseSchema, '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      // check if the user is able to update the row
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const { destinationKey, sourceKey, bucketName } = request.body

      if (!isValidKey(destinationKey)) {
        return response.status(400).send({
          statusCode: '400',
          error: 'Invalid key',
          message: 'The destination key contains invalid characters',
        })
      }

      const postgrest = getPostgrestClient(jwt)

      const objectResponse = await postgrest
        .from<Obj>('objects')
        .update({
          last_accessed_at: new Date().toISOString(),
          name: destinationKey,
        })
        .match({ bucket_id: bucketName, name: sourceKey })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        console.log(error)
        return response.status(400).send(transformPostgrestError(error, status))
      }

      // if successfully updated, copy and delete object from s3
      const oldS3Key = `${projectRef}/${bucketName}/${sourceKey}`
      const newS3Key = `${projectRef}/${bucketName}/${destinationKey}`

      // @todo what happens if one of these fail?
      await copyObject(client, globalS3Bucket, oldS3Key, newS3Key)
      await deleteObject(client, globalS3Bucket, oldS3Key)

      return response.status(200).send({
        message: 'Renamed',
      })
    }
  )
}
