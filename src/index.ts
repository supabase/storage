import fastify, { FastifyInstance } from 'fastify'
import { Server, IncomingMessage, ServerResponse } from 'http'
import autoload from 'fastify-autoload'
import path from 'path'

const app: FastifyInstance<Server, IncomingMessage, ServerResponse> = fastify({
  logger: true,
})

app.register(autoload, {
  dir: path.join(__dirname, 'routes'),
})

app.listen(8080, (err, address) => {
  if (err) {
    console.error(err)
    process.exit(1)
  }
  console.log(`Server listening at ${address}`)
})
