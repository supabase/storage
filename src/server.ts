import { FastifyInstance } from 'fastify'
import { IncomingMessage, Server, ServerResponse } from 'http'
import build from './app'
import buildAdmin from './admin-app'
import { getConfig } from './utils/config'
import { runMultitenantMigrations, runMigrations } from './utils/migrate'
import { cacheTenantConfigsFromDbAndRunMigrations } from './utils/tenant'

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
  const { isMultitenant } = getConfig()
  if (isMultitenant) {
    await runMultitenantMigrations()
    await cacheTenantConfigsFromDbAndRunMigrations()

    const adminApp: FastifyInstance<Server, IncomingMessage, ServerResponse> = buildAdmin({
      logger: loggerConfig,
    })

    try {
      await adminApp.listen(5001, '0.0.0.0')
    } catch (err) {
      adminApp.log.error(err)
      process.exit(1)
    }
  } else {
    await runMigrations()
  }

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
