import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import autoload from 'fastify-autoload'
import path from 'path'
import fastifyMultipart from 'fastify-multipart'
import fastifyCors from 'fastify-cors'
import fastifySwagger from 'fastify-swagger'
import { errorSchema } from './schemas/error'
import { authSchema } from './schemas/auth'

interface buildOpts extends FastifyServerOptions {
  exposeDocs?: boolean
}

const build = (opts: buildOpts = {}): FastifyInstance => {
  const app = fastify(opts)
  // @todo - should we set upload limits here?
  // https://github.com/fastify/fastify-multipart#handle-file-size-limitation
  app.register(fastifyMultipart)

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

  app.register(autoload, {
    dir: path.join(__dirname, 'routes'),
  })
  return app
}

export default build
