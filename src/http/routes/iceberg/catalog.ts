import { FastifyInstance } from 'fastify'
import { AuthenticatedRequest } from '../../types'
import { FromSchema } from 'json-schema-to-ts'
import { ERRORS } from '@internal/errors'
import { ROUTE_OPERATIONS } from '../operations'

const getConfigSchema = {
  type: 'object',
  querystring: {
    type: 'object',
    properties: {
      warehouse: { type: 'string', examples: ['my-warehouse'] },
    },
    required: ['warehouse'],
  },
  summary: 'Get Iceberg catalog configuration',
} as const

interface getConfigRequest extends AuthenticatedRequest {
  Querystring: FromSchema<(typeof getConfigSchema)['querystring']>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.get<getConfigRequest>(
    '/config',
    {
      config: {
        operation: { type: ROUTE_OPERATIONS.ICEBERG_GET_CONFIG },
      },
      schema: {
        ...getConfigSchema,
        tags: ['iceberg'],
      },
    },
    async (request, response) => {
      if (!request.icebergCatalog) {
        throw ERRORS.FeatureNotEnabled('icebergCatalog', 'iceberg_catalog')
      }

      const result = await request.icebergCatalog.getConfig({
        warehouse: request.query.warehouse,
      })

      return response.send(result)
    }
  )
}
