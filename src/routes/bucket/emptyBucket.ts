import { FastifyInstance } from 'fastify'
import { getPostgrestClient } from '../../utils'
import { deleteObjects, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { genericBucketRequest, Obj, Bucket } from '../../types/types'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

export default async function routes(fastify: FastifyInstance) {
  fastify.post<genericBucketRequest>('/:bucketId/empty', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }

    const jwt = authHeader.substring('Bearer '.length)
    const { bucketId } = request.params
    const postgrest = getPostgrestClient(jwt)

    const { data: bucket, error: bucketError, status: bucketStatus } = await postgrest
      .from<Bucket>('buckets')
      .select('name')
      .eq('id', bucketId)
      .single()

    console.log(bucket, bucketError)
    if (bucketError) {
      return response.status(bucketStatus).send(bucketError.message)
    }
    if (!bucket) {
      throw 'Should never happen'
    }
    const bucketName = bucket.name

    // @todo add pagination
    const { data: objects, error: objectError } = await postgrest
      .from<Obj>('objects')
      .select('name, id')
      .eq('bucketId', bucketId)
      .limit(1000)

    console.log(objects, objectError)
    if (objects) {
      const params = objects.map((ele) => {
        return {
          Key: `${projectRef}/${bucketName}/${ele.name}`,
        }
      })
      console.log(params)
      await deleteObjects(client, globalS3Bucket, params)

      const { error: deleteError } = await postgrest
        .from<Obj>('objects')
        .delete()
        .in(
          'id',
          objects.map((ele) => ele.id)
        )
      console.log(deleteError)
    }

    return response.status(200).send('Emptied')
  })
}
