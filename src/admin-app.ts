import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import { default as metrics } from 'fastify-metrics'
import tenantRoutes from './routes/tenant'

const build = (opts: FastifyServerOptions = {}): FastifyInstance => {
  const app = fastify(opts)
  app.register(tenantRoutes, { prefix: 'tenants' })
  app.register(metrics, {
    endpoint: '/metrics',
    enableRouteMetrics: false,
    blacklist: ['nodejs_version_info', 'process_start_time_seconds'],
  })
  app.get('/status', async (_, response) => response.status(200).send())
  return app
}

export default build
