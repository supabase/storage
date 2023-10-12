import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import fastifyMultipart from '@fastify/multipart'
import fastifySwagger from '@fastify/swagger'
import fastifySwaggerUi from '@fastify/swagger-ui'
import { routes, schemas, plugins, setErrorHandler } from './http'
import { getConfig } from './config'

interface buildOpts extends FastifyServerOptions {
  exposeDocs?: boolean
}

const { version, keepAliveTimeout, headersTimeout, isMultitenant } = getConfig()

const build = (opts: buildOpts = {}): FastifyInstance => {
  const app = fastify(opts)
  app.register(fastifyMultipart, {
    limits: {
      fields: 10,
      files: 1,
    },
    throwFileSizeLimit: false,
  })

  app.addContentTypeParser('*', function (request, payload, done) {
    done(null)
  })

  app.server.keepAliveTimeout = keepAliveTimeout * 1000
  app.server.headersTimeout = headersTimeout * 1000

  // kong should take care of cors
  // app.register(fastifyCors)

  if (opts.exposeDocs) {
    app.register(fastifySwagger, {
      openapi: {
        info: {
          title: 'Supabase Storage API',
          description: 'API documentation for Supabase Storage',
          version: '0.0.1',
        },
        tags: [
          { name: 'object', description: 'Object end-points' },
          { name: 'bucket', description: 'Bucket end-points' },
          { name: 'deprecated', description: 'Deprecated end-points' },
        ],
      },
    })
    app.register(fastifySwaggerUi, {
      routePrefix: '/documentation',
    })
  }

  // add in common schemas
  app.addSchema(schemas.authSchema)
  app.addSchema(schemas.errorSchema)

  app.register(plugins.tenantId)
  app.register(plugins.metrics({ enabledEndpoint: !isMultitenant }))
  app.register(plugins.logTenantId)
  app.register(plugins.logRequest({ excludeUrls: ['/status', '/metrics', '/health'] }))
  app.register(routes.multiPart, { prefix: 'upload/resumable' })
  app.register(routes.bucket, { prefix: 'bucket' })
  app.register(routes.object, { prefix: 'object' })
  app.register(routes.render, { prefix: 'render/image' })
  app.register(routes.healthcheck, { prefix: 'health' })

  setErrorHandler(app)

  app.get('/version', (_, reply) => {
    reply.send(version)
  })
  app.get('/status', async (request, response) => response.status(200).send())

  return app
}

export default build
