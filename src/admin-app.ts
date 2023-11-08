import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import { routes, plugins, setErrorHandler } from './http'
import { Registry } from 'prom-client'

const build = (opts: FastifyServerOptions = {}, appInstance?: FastifyInstance): FastifyInstance => {
  const app = fastify(opts)
  app.register(plugins.adminTenantId)
  app.register(plugins.logTenantId)
  app.register(plugins.logRequest({ excludeUrls: ['/status', '/metrics', '/health'] }))
  app.register(routes.tenant, { prefix: 'tenants' })

  let registriesToMerge: Registry[] = []

  if (appInstance) {
    app.get('/metrics', async (req, reply) => {
      if (registriesToMerge.length === 0) {
        const globalRegistry = appInstance.metrics.client.register
        const defaultRegistries = (appInstance.metrics as any).getCustomDefaultMetricsRegistries()
        const routeRegistries = (appInstance.metrics as any).getCustomRouteMetricsRegistries()

        registriesToMerge = Array.from(
          new Set([globalRegistry, ...defaultRegistries, ...routeRegistries])
        )
      }

      if (registriesToMerge.length === 1) {
        const data = await registriesToMerge[0].metrics()
        return reply.type(registriesToMerge[0].contentType).send(data)
      }
      const merged = appInstance.metrics.client.Registry.merge(registriesToMerge)

      const data = await merged.metrics()

      return reply.type(merged.contentType).send(data)
    })
  }

  app.get('/status', async (_, response) => response.status(200).send())

  setErrorHandler(app)

  return app
}

export default build
