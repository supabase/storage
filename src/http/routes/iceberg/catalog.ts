import { FastifyInstance } from 'fastify'
import { getConfig } from '../../../config'
import { AuthenticatedRequest } from '../../types'
import { FromSchema } from 'json-schema-to-ts'
import { ERRORS } from '@internal/errors'

const { icebergWarehouse } = getConfig()

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
      schema: {
        ...getConfigSchema,
        tags: ['iceberg'],
      },
    },
    async (request, response) => {
      if (!request.icebergCatalog) {
        throw ERRORS.FeatureNotEnabled('icebergCatalog', 'iceberg_catalog')
      }

      const bucket = await request.icebergCatalog.findCatalogById({
        tenantId: request.tenantId,
        id: request.query.warehouse,
      })

      const result = await request.icebergCatalog.getConfig({
        warehouse: bucket.id,
      })

      return response.send(result)
    }
  )
}
