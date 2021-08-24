import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import apiKey from './plugins/apikey'
import tenantRoutes from './routes/tenant'

const build = (opts: FastifyServerOptions = {}): FastifyInstance => {
  const app = fastify(opts)
  app.register(apiKey)
  app.register(tenantRoutes, { prefix: 'tenants' })
  return app
}

export default build
