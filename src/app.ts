import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import fastifyMultipart from 'fastify-multipart'
import fastifyCors from 'fastify-cors'
import fastifySwagger from 'fastify-swagger'
import { errorSchema } from './schemas/error'
import { authSchema } from './schemas/auth'

import bucketRoutes from './routes/bucket/'
import objectRoutes from './routes/object'
import searchRoutes from './routes/search'

interface buildOpts extends FastifyServerOptions {
  exposeDocs?: boolean
}

const build = (opts: buildOpts = {}): FastifyInstance => {
  const app = fastify(opts)
  app.register(fastifyMultipart, {
    limits: {
      fields: 10,
      fileSize: 50 * 1024 * 1024,
      files: 1,
    },
  })

  // @todo - restrict origin here
  app.register(fastifyCors)

  if (opts.exposeDocs) {
    app.register(fastifySwagger, {
      exposeRoute: true,
      swagger: {
        info: {
          title: 'Supabase Storage API',
          description: 'API documentation for Supabase Storage',
          version: '0.0.1',
        },
      },
    })
  }

  // add in common schemas
  app.addSchema(authSchema)
  app.addSchema(errorSchema)

  app.register(bucketRoutes, { prefix: 'bucket' })
  app.register(objectRoutes, { prefix: 'object' })
  app.register(searchRoutes, { prefix: 'search' })

  return app
}

export default build
