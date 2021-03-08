import { FastifyInstance } from 'fastify'
import { getPostgrestClient } from '../../utils'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest } from '../../types/types'

const searchRequestParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string' },
  },
  required: ['bucketName'],
} as const
const searchRequestBodySchema = {
  type: 'object',
  properties: {
    prefix: { type: 'string' },
    limit: { type: 'number' },
    offset: { type: 'number' },
  },
  required: ['prefix'],
} as const
// @todo make better
const successResponseSchema = {
  type: 'array',
  items: {
    type: 'object',
    properties: {
      folder: { type: 'string' },
      id: { anyOf: [{ type: 'string' }, { type: 'null' }] },
      updatedAt: { anyOf: [{ type: 'string' }, { type: 'null' }] },
      createdAt: { anyOf: [{ type: 'string' }, { type: 'null' }] },
      lastAccessedAt: { anyOf: [{ type: 'string' }, { type: 'null' }] },
      metadata: { anyOf: [{ type: 'object', additionalProperties: true }, { type: 'null' }] },
    },
  },
}
interface searchRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof searchRequestBodySchema>
  Params: FromSchema<typeof searchRequestParamsSchema>
}
// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Search for objects under a prefix'
  fastify.post<searchRequestInterface>(
    '/:bucketName',
    {
      schema: {
        body: searchRequestBodySchema,
        params: searchRequestParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { 200: successResponseSchema, '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      const authHeader = request.headers.authorization
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
      const { data: results, error, status } = await postgrest.rpc('search', {
        prefix,
        bucketname: bucketName,
        limits: limit,
        offsets: offset,
        levels: prefix.split('/').length,
      })
      console.log(results, error)
      if (error) {
        return response.status(status).send({
          statusCode: error.code,
          error: error.details,
          message: error.message,
        })
      }

      response.status(200).send(results)
    }
  )
}
