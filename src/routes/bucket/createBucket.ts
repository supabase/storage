import { FastifyInstance } from 'fastify'
import { getPostgrestClient, getOwner } from '../../utils'
import { bucketCreateRequest, Bucket } from '../../types/types'

export default async function routes(fastify: FastifyInstance) {
  fastify.post<bucketCreateRequest>('/', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }

    const jwt = authHeader.substring('Bearer '.length)
    const postgrest = getPostgrestClient(jwt)
    const owner = await getOwner(jwt)

    const { name: bucketName } = request.body

    const { data: results, error } = await postgrest.from<Bucket>('buckets').insert([
      {
        name: bucketName,
        owner,
      },
    ])
    console.log(results, error)
    return response.status(200).send(results)
  })
}
