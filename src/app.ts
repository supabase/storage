import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import fastifyMultipart from '@fastify/multipart'
import fastifySwagger from '@fastify/swagger'
import bucketRoutes from './routes/bucket/'
import objectRoutes from './routes/object'
import { authSchema } from './schemas/auth'
import { errorSchema } from './schemas/error'
import logTenantId from './plugins/log-tenant-id'
import tenantId from './plugins/tenant-id'
import logRequest from './plugins/log-request'

interface buildOpts extends FastifyServerOptions {
  exposeDocs?: boolean
}

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

  // kong should take care of cors
  // app.register(fastifyCors)

  if (opts.exposeDocs) {
    app.register(fastifySwagger, {
      exposeRoute: true,
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
  }

  // add in common schemas
  app.addSchema(authSchema)
  app.addSchema(errorSchema)

  app.register(tenantId)
  app.register(logTenantId)
  app.register(logRequest({ excludeUrls: ['/status'] }))
  app.register(bucketRoutes, { prefix: 'bucket' })
  app.register(objectRoutes, { prefix: 'object' })

  app.get('/status', async (request, response) => response.status(200).send())

  return app
}

export default build
