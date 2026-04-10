import fastifySwagger from '@fastify/swagger'
import fastifySwaggerUi from '@fastify/swagger-ui'
import { handleMetricsRequest } from '@internal/monitoring/otel-metrics'
import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import { getConfig } from './config'
import { plugins, routes, setErrorHandler } from './http'

interface buildOpts extends FastifyServerOptions {
  exposeDocs?: boolean
}

const { version, prometheusMetricsEnabled } = getConfig()

const build = (opts: buildOpts = {}): FastifyInstance => {
  const app = fastify(opts)

  if (opts.exposeDocs) {
    app.register(fastifySwagger, {
      exposeHeadRoutes: true,
      openapi: {
        info: {
          title: 'Supabase Storage Admin API',
          description: 'Admin API documentation for Supabase Storage',
          version,
        },
        tags: [
          { name: 'tenant', description: 'Tenant management' },
          { name: 'object', description: 'Object management' },
          { name: 'jwks', description: 'JWKS configuration' },
          { name: 'migration', description: 'Database migrations' },
          { name: 's3-credentials', description: 'S3 credentials management' },
          { name: 'queue', description: 'Queue management' },
          { name: 'metrics', description: 'Metrics configuration' },
        ],
      },
    })

    app.register(fastifySwaggerUi, {
      routePrefix: '/documentation',
    })
  }

  app.register(plugins.signals)
  app.register(plugins.adminTenantId)
  app.register(plugins.logRequest({ excludeUrls: ['/status', '/metrics', '/health', '/version'] }))
  app.register(routes.tenants, { prefix: 'tenants' })
  app.register(routes.objects, { prefix: 'tenants' })
  app.register(routes.jwks, { prefix: 'tenants' })
  app.register(routes.migrations, { prefix: 'migrations' })
  app.register(routes.s3Credentials, { prefix: 's3' })
  app.register(routes.queue, { prefix: 'queue' })
  app.register(routes.metricsConfig, { prefix: 'metrics' })

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
