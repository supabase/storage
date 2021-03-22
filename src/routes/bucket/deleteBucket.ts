import { FastifyInstance } from 'fastify'
import { getPostgrestClient, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { Obj, Bucket, AuthenticatedRequest } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

const { serviceKey } = getConfig()

const deleteBucketParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string' },
  },
  required: ['bucketId'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string' },
  },
}
interface deleteBucketRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof deleteBucketParamsSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Delete a bucket'
  fastify.delete<deleteBucketRequestInterface>(
    '/:bucketId',
    {
      schema: {
        params: deleteBucketParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { 200: successResponseSchema, '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)
      const { bucketId } = request.params
      const userPostgrest = getPostgrestClient(jwt)
      const superUserPostgrest = getPostgrestClient(serviceKey)

      const {
        data: bucketResults,
        error: bucketError,
        status: bucketStatus,
      } = await userPostgrest.from<Bucket>('buckets').select('id').eq('id', bucketId).single()

      console.log(bucketResults, bucketError)

      if (bucketError) {
        return response.status(400).send(transformPostgrestError(bucketError, bucketStatus))
      }

      const { count: objectCount, error: objectError } = await superUserPostgrest
        .from<Obj>('objects')
        .select('id', { count: 'exact' })
        .eq('bucket_id', bucketId)
        .limit(10)

      console.log(objectCount, objectError)
      if (objectCount && objectCount > 0) {
        return response.status(400).send({
          statusCode: '400',
          message: 'Bucket must be empty before you can delete it',
          error: 'Bucket not empty',
        })
      }

      const { data: results, error, status } = await userPostgrest
        .from<Bucket>('buckets')
        .delete()
        .eq('id', bucketId)
      console.log(results, error)
      if (error) {
        return response.status(400).send(transformPostgrestError(error, status))
      }
      return response.status(200).send({ message: 'Deleted' })
    }
  )
}
