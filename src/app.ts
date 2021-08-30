import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import fastifyMultipart from 'fastify-multipart'
import fastifySwagger from 'fastify-swagger'
import underPressure from 'under-pressure'
import bucketRoutes from './routes/bucket/'
import objectRoutes from './routes/object'
import { authSchema } from './schemas/auth'
import { errorSchema } from './schemas/error'
import { getConfig } from './utils/config'
import logTenantId from './plugins/log-tenant-id'
import tenantId from './plugins/tenant-id'

interface buildOpts extends FastifyServerOptions {
  exposeDocs?: boolean
}

const build = (opts: buildOpts = {}): FastifyInstance => {
  const app = fastify(opts)
  const { fileSizeLimit } = getConfig()
  app.register(fastifyMultipart, {
    limits: {
      fields: 10,
      fileSize: fileSizeLimit,
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
      swagger: {
        info: {
          title: 'Supabase Storage API',
          description: 'API documentation for Supabase Storage',
          version: '0.0.1',
        },
        tags: [
          { name: 'object', description: 'Object end-points' },
          { name: 'bucket', description: 'Bucket end-points' },
        ],
      },
    })
  }

  // add in common schemas
  app.addSchema(authSchema)
  app.addSchema(errorSchema)

  app.register(tenantId)
  app.register(logTenantId)
  app.register(bucketRoutes, { prefix: 'bucket' })
  app.register(objectRoutes, { prefix: 'object' })
  app.register(underPressure, { exposeStatusRoute: true, maxEventLoopUtilization: 0.99 })

  return app
}

export default build
