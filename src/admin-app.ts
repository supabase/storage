import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import tenantRoutes from './routes/tenant'

const build = (opts: FastifyServerOptions = {}): FastifyInstance => {
  const app = fastify(opts)
  app.register(tenantRoutes, { prefix: 'tenants' })
  app.get('/status', async (request, response) => response.status(200).send())
  return app
}

export default build
