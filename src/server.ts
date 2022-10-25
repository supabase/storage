import { FastifyInstance } from 'fastify'
import { IncomingMessage, Server, ServerResponse } from 'http'

import build from './app'
import buildAdmin from './admin-app'
import { getConfig } from './config'
import { runMultitenantMigrations, runMigrations } from './database/migrate'
import { listenForTenantUpdate } from './database/tenant'
import { logger } from './monitoring'
import { Queue } from './queue'

const exposeDocs = true

;(async () => {
  const { isMultitenant, requestIdHeader, adminRequestIdHeader } = getConfig()
  if (isMultitenant) {
    await runMultitenantMigrations()
    await listenForTenantUpdate()

    const adminApp: FastifyInstance<Server, IncomingMessage, ServerResponse> = buildAdmin({
      logger,
      disableRequestLogging: true,
      requestIdHeader: adminRequestIdHeader,
    })

    try {
      await adminApp.listen({ port: 5001, host: '0.0.0.0' })
    } catch (err) {
      adminApp.log.error(err)
      process.exit(1)
    }
  } else {
    await runMigrations()
  }

  await Queue.init()

  const app: FastifyInstance<Server, IncomingMessage, ServerResponse> = build({
    logger,
    disableRequestLogging: true,
    exposeDocs,
    requestIdHeader,
  })

  app.listen(
    {
      port: 5000,
      host: '0.0.0.0',
    },
    (err, address) => {
      if (err) {
        console.error(err)
        process.exit(1)
      }
      console.log(`Server listening at ${address}`)
    }
  )
})()
