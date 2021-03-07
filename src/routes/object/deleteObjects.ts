import { FastifyInstance } from 'fastify'
import { getPostgrestClient } from '../../utils'
import { deleteObjects, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const deleteObjectsParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string' },
  },
  required: ['bucketName'],
} as const
const deleteObjectsBodySchema = {
  type: 'object',
  properties: {
    prefixes: { type: 'array', items: { type: 'string' }, minItems: 1, maxItems: 1000 },
  },
  required: ['prefixes'],
} as const
interface deleteObjectsInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof deleteObjectsParamsSchema>
  Body: FromSchema<typeof deleteObjectsBodySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  fastify.delete<deleteObjectsInterface>(
    '/:bucketName',
    {
      schema: {
        body: deleteObjectsBodySchema,
        params: deleteObjectsParamsSchema,
        headers: { $ref: 'authSchema#' },
      },
    },
    async (request, response) => {
      // check if the user is able to insert that row
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const { bucketName } = request.params
      const prefixes = request.body['prefixes']
      if (!prefixes) {
        return response.status(400).send('prefixes is required')
      }

      const postgrest = getPostgrestClient(jwt)
      // @todo how to merge these into one query?
      // i can create a view and add INSTEAD OF triggers..is that the way to do it?
      // @todo add unique constraint for just bucket names
      const { data: bucket, error: bucketError, status: bucketStatus } = await postgrest
        .from('buckets')
        .select('id')
        .eq('name', bucketName)
        .single()

      console.log(bucket, bucketError)
      if (bucketError) {
        return response.status(bucketStatus).send(bucketError.message)
      }

      const objectResponse = await postgrest
        .from<Obj>('objects')
        .delete()
        .eq('bucketId', bucket.id)
        .in('name', prefixes)

      if (objectResponse.error) {
        const { error, status } = objectResponse
        console.log(error)
        return response.status(status).send(error.message)
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
