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
      schema: getConfigSchema,
    },
    async (request, response) => {
      if (!icebergWarehouse) {
        throw ERRORS.FeatureNotEnabled('icebergWarehouse', 'iceberg_catalog')
      }

      const bucket = await request.storage.findBucket(
        request.query.warehouse,
        'name,iceberg_catalog'
      )

      if (!bucket.iceberg_catalog) {
        throw ERRORS.FeatureNotEnabled(request.query.warehouse, 'iceberg_catalog')
      }

      const result = await request.icebergCatalog?.getConfig({
        warehouse: bucket.name,
      })

      return response.send(result)
    }
  )
}
