import fastify, { FastifyInstance } from 'fastify'
import autoload from 'fastify-autoload'
import path from 'path'
import fastifyMultipart from 'fastify-multipart'
import fastifyCors from 'fastify-cors'

const build = (opts = {}): FastifyInstance => {
  const app = fastify(opts)
  // @todo - should we set upload limits here?
  // https://github.com/fastify/fastify-multipart#handle-file-size-limitation
  app.register(fastifyMultipart)

  // @todo - restrict origin here
  app.register(fastifyCors)

  app.register(autoload, {
    dir: path.join(__dirname, 'routes'),
  })
  return app
}

export default build
