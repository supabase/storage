import { FastifyInstance } from 'fastify'
import { Server, IncomingMessage, ServerResponse } from 'http'
import build from './app'

const loggerConfig = {
  prettyPrint: true,
}
let exposeDocs = true
if (process.env.NODE_ENV === 'production') {
  loggerConfig.prettyPrint = false
  exposeDocs = true // @todo change
}
const app: FastifyInstance<Server, IncomingMessage, ServerResponse> = build({
  logger: loggerConfig,
  exposeDocs,
})

app.listen(5000, '0.0.0.0', (err, address) => {
  if (err) {
    console.error(err)
    process.exit(1)
  }
  console.log(`Server listening at ${address}`)
})
