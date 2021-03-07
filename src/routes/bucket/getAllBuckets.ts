import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { getPostgrestClient } from '../../utils'
import { Bucket } from '../../types/types'

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  // @todo I have enabled RLS only for objects table
  // makes writing the access policies a bit easier
  // tradeoff is that everyone can see what buckets there are
  // ahh looks like we do need RLS since users would be able to delete and empty buckets then
  // probably a RLS policy with just read permissions?
  fastify.get<RequestGenericInterface>('/', async (request, response) => {
    // get list of all buckets
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const postgrest = getPostgrestClient(jwt)
    const { data: results, error, status } = await postgrest.from<Bucket>('buckets').select('*')
    console.log(results, error)

    if (error) {
      return response.status(status).send(error.message)
    }

    response.send(results)
  })
}
