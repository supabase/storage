import { FastifyInstance } from 'fastify'
import { getPostgrestClient, transformPostgrestError } from '../../utils'
import { deleteObject, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { Obj, Bucket, AuthenticatedRequest } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

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
  type: 'string',
}
interface deleteObjectRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof deleteObjectParamsSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Delete an object'
  fastify.delete<deleteObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema: {
        params: deleteObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { 200: successResponseSchema, '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      // check if the user is able to insert that row
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const { bucketName } = request.params
      const objectName = request.params['*']

      const postgrest = getPostgrestClient(jwt)
      // @todo how to merge these into one query?
      // i can create a view and add INSTEAD OF triggers..is that the way to do it?
      // @todo add unique constraint for just bucket names
      const bucketResponse = await postgrest
        .from<Bucket>('buckets')
        .select('id')
        .eq('name', bucketName)
        .single()

      if (bucketResponse.error) {
        const { error, status } = bucketResponse
        console.log(error)
        return response.status(status).send({
          statusCode: '404',
          error: 'Not found',
          message: 'The requested bucket was not found',
        })
      }
      console.log(bucketResponse.body)
      const { data: bucket } = bucketResponse

      // todo what if objectName is * or something
      const objectResponse = await postgrest
        .from<Obj>('objects')
        .delete()
        .match({
          name: objectName,
          bucketId: bucket.id,
        })
        .single()

      if (objectResponse.error) {
        const { error, status } = objectResponse
        console.log(error)
        return response.status(400).send(transformPostgrestError(error, status))
      }
      const { data: results } = objectResponse
      console.log(results)

      // if successfully deleted, delete from s3 too
      const s3Key = `${projectRef}/${bucketName}/${objectName}`
      await deleteObject(client, globalS3Bucket, s3Key)

      return response.status(200).send('Deleted')
    }
  )
}
