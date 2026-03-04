import { getMetricsConfig, setMetricsEnabled } from '@internal/monitoring/metrics'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import apiKey from '../../plugins/apikey'

const updateMetricsConfigSchema = {
  body: {
    type: 'object',
    properties: {
      metrics: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            name: { type: 'string' },
            enabled: { type: 'boolean' },
          },
          required: ['name', 'enabled'],
        },
      },
    },
    required: ['metrics'],
  },
} as const

interface UpdateMetricsConfigRequest extends RequestGenericInterface {
  Body: FromSchema<typeof updateMetricsConfigSchema.body>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.get('/config', async (_request, reply) => {
    return reply.send({
      metrics: getMetricsConfig(),
    })
  })

  fastify.put<UpdateMetricsConfigRequest>(
    '/config',
    { schema: updateMetricsConfigSchema },
    async (request, reply) => {
      setMetricsEnabled(request.body.metrics)
      return reply.code(200).send({
        metrics: getMetricsConfig(),
      })
    }
  )
}
