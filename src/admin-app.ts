import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import { default as metrics } from 'fastify-metrics'
import { Registry } from 'prom-client'
import { routes, plugins } from './http'

export interface AdminOptions {
  register?: Registry
}

const build = (opts: FastifyServerOptions = {}, adminOpts: AdminOptions = {}): FastifyInstance => {
  const app = fastify(opts)
  app.register(plugins.logRequest({ excludeUrls: ['/status'] }))
  app.register(routes.tenant, { prefix: 'tenants' })
  app.register(metrics, {
    endpoint: '/metrics',
    defaultMetrics: {
      enabled: true,
      register: adminOpts.register,
    },
    routeMetrics: {
      enabled: true,
      registeredRoutesOnly: true,
      groupStatusCodes: true,
    },
  })
  app.get('/status', async (_, response) => response.status(200).send())
  return app
}

export default build
