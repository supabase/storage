import { FastifyInstance } from 'fastify'
import { getPostgrestClient } from '../../utils'
import { searchRequest } from '../../types/types'

export default async function routes(fastify: FastifyInstance) {
  fastify.post<searchRequest>('/:bucketName', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const postgrest = getPostgrestClient(jwt)
    const { bucketName } = request.params
    const { limit, offset } = request.body
    let { prefix } = request.body
    if (prefix.length > 0 && !prefix.endsWith('/')) {
      // assuming prefix is always a folder
      prefix = `${prefix}/`
    }
    console.log(request.body)
    console.log(`searching for `, prefix)
    const { data: results, error } = await postgrest.rpc('search', {
      prefix,
      bucketname: bucketName,
      limits: limit,
      offsets: offset,
      levels: prefix.split('/').length,
    })
    console.log(results, error)

    response.status(200).send(results)
  })
}
