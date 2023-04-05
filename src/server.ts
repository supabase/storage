import { FastifyInstance } from 'fastify'
import { IncomingMessage, Server, ServerResponse } from 'http'

import build from './app'
import buildAdmin from './admin-app'
import { getConfig } from './config'
import { runMultitenantMigrations, runMigrations } from './database/migrate'
import { listenForTenantUpdate } from './database/tenant'
import { logger } from './monitoring'
import { Queue } from './queue'
import { normalizeRawError } from './storage'
import { TenantConnection } from './database/connection'

const exposeDocs = true

;(async () => {
  const {
    isMultitenant,
    requestIdHeader,
    adminRequestIdHeader,
    adminPort,
    port,
    host,
    enableQueueEvents,
  } = getConfig()

  if (isMultitenant) {
    await runMultitenantMigrations()
    await listenForTenantUpdate()
  } else {
    await runMigrations()
  }

  if (enableQueueEvents) {
    await Queue.init()
  }

  const app: FastifyInstance<Server, IncomingMessage, ServerResponse> = build({
    logger,
    disableRequestLogging: true,
    exposeDocs,
    requestIdHeader,
  })

  app.listen({ port, host }, (err, address) => {
    if (err) {
      console.error(err)
      process.exit(1)
    }
    console.log(`Server listening at ${address}`)
  })

  if (isMultitenant) {
    const adminApp: FastifyInstance<Server, IncomingMessage, ServerResponse> = buildAdmin(
      {
        logger,
        disableRequestLogging: true,
        requestIdHeader: adminRequestIdHeader,
      },
      app
    )

    try {
      await adminApp.listen({ port: adminPort, host })
    } catch (err) {
      adminApp.log.error(err)
      process.exit(1)
    }
  }

  process.on('uncaughtException', (e) => {
    logger.error(
      {
        error: normalizeRawError(e),
      },
      'uncaught exception'
    )
    process.exit(1)
  })

  process.on('SIGTERM', async () => {
    try {
      await app.close()
      await Promise.allSettled([Queue.stop(), TenantConnection.stop()])
      process.exit(0)
    } catch (e) {
      logger.error('shutdown error', { error: e })
      process.exit(1)
    }
  })
})()
