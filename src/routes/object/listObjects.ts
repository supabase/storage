import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { objectSchema } from '../../schemas/object'
import { AuthenticatedRequest } from '../../types/types'
import { transformPostgrestError } from '../../utils'
import { createDefaultSchema } from '../../utils/generic-routes'

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
    prefix: { type: 'string', examples: ['folder/subfolder'] },
    limit: { type: 'integer', minimum: 1, examples: [10] },
    offset: { type: 'integer', minimum: 0, examples: [0] },
    sortBy: {
      type: 'object',
      properties: {
        column: { type: 'string', enum: ['name', 'updated_at', 'created_at', 'last_accessed_at'] },
        order: { type: 'string', enum: ['asc', 'desc'] },
      },
      required: ['column'],
    },
    search: {
      type: 'string',
    },
  },
  required: ['prefix'],
} as const
const successResponseSchema = {
  type: 'array',
  items: objectSchema,
}
interface searchRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof searchRequestBodySchema>
  Params: FromSchema<typeof searchRequestParamsSchema>
}
// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Search for objects under a prefix'

  const schema = createDefaultSchema(successResponseSchema, {
    body: searchRequestBodySchema,
    params: searchRequestParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.post<searchRequestInterface>(
    '/list/:bucketName',
    {
      schema,
    },
    async (request, response) => {
      const { bucketName } = request.params
      const { limit, offset, sortBy, search } = request.body
      let sortColumn, sortOrder
      if (sortBy?.column) {
        sortColumn = sortBy.column
        sortOrder = sortBy.order ?? 'asc'
      } else {
        sortColumn = 'name'
        sortOrder = 'asc'
      }
      let { prefix } = request.body
      if (prefix.length > 0 && !prefix.endsWith('/')) {
        // assuming prefix is always a folder
        prefix = `${prefix}/`
      }
      request.log.info(request.body)
      request.log.info(`searching for %s`, prefix)

      const {
        data: results,
        error,
        status,
      } = await request.postgrest.rpc('search', {
        prefix,
        bucketname: bucketName,
        limits: limit,
        offsets: offset,
        levels: prefix.split('/').length,
        search,
        sortcolumn: sortColumn,
        sortorder: sortOrder,
      })

      if (error) {
        request.log.error({ error }, 'search rpc')
        return response.status(status).send(transformPostgrestError(error, status))
      }
      request.log.info({ results }, 'results')

      response.status(200).send(results)
    }
  )
}
