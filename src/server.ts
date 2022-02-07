import { FastifyInstance } from 'fastify'
import { IncomingMessage, Server, ServerResponse } from 'http'
import pino from 'pino'

import build from './app'
import buildAdmin from './admin-app'
import { getConfig } from './utils/config'
import { runMultitenantMigrations, runMigrations } from './utils/migrate'
import { cacheTenantConfigsFromDbAndRunMigrations } from './utils/tenant'

const logger = pino({
  formatters: {
    level(label) {
      return { level: label }
    },
  },
  timestamp: pino.stdTimeFunctions.isoTime,
})

const exposeDocs = true

;(async () => {
  const { isMultitenant } = getConfig()
  if (isMultitenant) {
    await runMultitenantMigrations()
    await cacheTenantConfigsFromDbAndRunMigrations()

    const adminApp: FastifyInstance<Server, IncomingMessage, ServerResponse> = buildAdmin({
      logger,
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

  const app: FastifyInstance<Server, IncomingMessage, ServerResponse> = build({
    logger,
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
