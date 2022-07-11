import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import { default as metrics } from 'fastify-metrics'
import tenantRoutes from './routes/tenant'
import { Registry } from 'prom-client'

export interface AdminOptions {
  register?: Registry
}

const build = (opts: FastifyServerOptions = {}, adminOpts: AdminOptions = {}): FastifyInstance => {
  const app = fastify(opts)
  app.register(tenantRoutes, { prefix: 'tenants' })
  app.register(metrics, {
    endpoint: '/metrics',
    enableRouteMetrics: true,
    blacklist: ['nodejs_version_info', 'process_start_time_seconds'],
    register: adminOpts.register,
  })
  app.get('/status', async (_, response) => response.status(200).send())
  return app
}

export default build
