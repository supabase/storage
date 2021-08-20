import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import tenantRoutes from './routes/tenant'

const build = (opts: FastifyServerOptions = {}): FastifyInstance => {
  const app = fastify(opts)
  app.register(tenantRoutes, { prefix: 'tenants' })
  return app
}

export default build
