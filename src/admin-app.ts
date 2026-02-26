import { handleMetricsRequest } from '@internal/monitoring/otel-metrics'
import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import { getConfig } from './config'
import { plugins, routes, setErrorHandler } from './http'

const { version, prometheusMetricsEnabled } = getConfig()

const build = (opts: FastifyServerOptions = {}): FastifyInstance => {
  const app = fastify(opts)
  app.register(plugins.signals)
  app.register(plugins.adminTenantId)
  app.register(plugins.logRequest({ excludeUrls: ['/status', '/metrics', '/health', '/version'] }))
  app.register(routes.tenants, { prefix: 'tenants' })
  app.register(routes.objects, { prefix: 'tenants' })
  app.register(routes.jwks, { prefix: 'tenants' })
  app.register(routes.migrations, { prefix: 'migrations' })
  app.register(routes.s3Credentials, { prefix: 's3' })
  app.register(routes.queue, { prefix: 'queue' })

  // Register /metrics endpoint - uses OTel Prometheus exporter
  if (prometheusMetricsEnabled) {
    app.get('/metrics', handleMetricsRequest)
  }

  app.get('/version', (_, reply) => {
    reply.send(version)
  })
  app.get('/status', async (_, response) => response.status(200).send())

  setErrorHandler(app)

  return app
}

export default build
