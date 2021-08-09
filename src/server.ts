import { FastifyInstance } from 'fastify'
import { IncomingMessage, Server, ServerResponse } from 'http'
import build from './app'
import { runMigrations } from './utils/migrate'

const loggerConfig = {
  prettyPrint: true,
  level: 'info',
}
const exposeDocs = true
if (process.env.NODE_ENV === 'production') {
  loggerConfig.prettyPrint = false
  loggerConfig.level = 'error'
}

;(async () => {
  // await runMigrations()
  const app: FastifyInstance<Server, IncomingMessage, ServerResponse> = await build({
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
})()
