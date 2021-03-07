import { FastifyInstance } from 'fastify'
import { getPostgrestClient } from '../../utils'
import { getConfig } from '../../utils/config'
import { genericBucketRequest, Obj, Bucket } from '../../types/types'

const { serviceKey } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  fastify.delete<genericBucketRequest>('/:bucketId', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }

    const jwt = authHeader.substring('Bearer '.length)
    const { bucketId } = request.params
    const userPostgrest = getPostgrestClient(jwt)
    const superUserPostgrest = getPostgrestClient(serviceKey)

    const { count: objectCount, error: objectError } = await superUserPostgrest
      .from<Obj>('objects')
      .select('id', { count: 'exact' })
      .eq('bucketId', bucketId)

    console.log(objectCount, objectError)
    if (objectCount && objectCount > 0) {
      return response.status(400).send('Bucket not empty')
    }

    const { data: results, error } = await userPostgrest
      .from<Bucket>('buckets')
      .delete()
      .eq('id', bucketId)
    console.log(results, error)
    return response.status(200).send('Deleted')
  })
}
