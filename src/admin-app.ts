import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import underPressure from 'under-pressure'
import apiKey from './plugins/apikey'
import tenantRoutes from './routes/tenant'

const build = (opts: FastifyServerOptions = {}): FastifyInstance => {
  const app = fastify(opts)
  app.register(apiKey)
  app.register(tenantRoutes, { prefix: 'tenants' })
  app.register(underPressure, { exposeStatusRoute: true, maxEventLoopUtilization: 0.99 })
  return app
}

export default build
