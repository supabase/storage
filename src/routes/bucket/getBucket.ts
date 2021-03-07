import { FastifyInstance } from 'fastify'
import { getPostgrestClient } from '../../utils'
import { Bucket, genericBucketRequest } from '../../types/types'

export default async function routes(fastify: FastifyInstance) {
  fastify.get<genericBucketRequest>('/:bucketId', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)
    const { bucketId } = request.params
    const postgrest = getPostgrestClient(jwt)
    const { data: results, error, status } = await postgrest
      .from<Bucket>('buckets')
      .select('*')
      .eq('id', bucketId)
      .single()

    console.log(results, error)

    if (error) {
      return response.status(status).send(error.message)
    }

    response.send(results)
  })
}
