import { FastifyInstance } from 'fastify'
import { IncomingMessage, Server, ServerResponse } from 'http'

import build from './app'
import buildAdmin from './admin-app'
import { getConfig } from './config'
import { runMultitenantMigrations, runMigrationsOnTenant } from './database/migrate'
import { listenForTenantUpdate } from './database/tenant'
import { logger, logSchema } from './monitoring'
import { Queue } from './queue'
import { TenantConnection } from './database/connection'
import { PubSub } from './database/pubsub'

const exposeDocs = true

;(async () => {
  const {
    databaseURL,
    isMultitenant,
    requestTraceHeader,
    adminRequestIdHeader,
    adminPort,
    port,
    host,
    pgQueueEnable,
  } = getConfig()

  if (isMultitenant) {
    await runMultitenantMigrations()
    await listenForTenantUpdate(PubSub)
  } else {
    await runMigrationsOnTenant(databaseURL)
  }

  if (pgQueueEnable) {
    await Queue.init()
  }

  const app: FastifyInstance<Server, IncomingMessage, ServerResponse> = build({
    logger,
    disableRequestLogging: true,
    exposeDocs,
    requestIdHeader: requestTraceHeader,
  })

  await PubSub.connect()

  app.listen({ port, host }, (err, address) => {
    if (err) {
      logSchema.error(logger, `Server failed to start`, {
        type: 'serverStartError',
        error: err,
      })
      process.exit(1)
    }
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
      logSchema.error(adminApp.log, 'Failed to start admin app', {
        type: 'adminAppStartError',
        error: err,
      })
      process.exit(1)
    }
  }

  process.on('uncaughtException', (e) => {
    logSchema.error(logger, 'uncaught exception', {
      type: 'uncaughtException',
      error: e,
    })
    process.exit(1)
  })

  process.on('SIGTERM', async () => {
    try {
      await app.close()
      await Promise.allSettled([Queue.stop(), TenantConnection.stop(), PubSub.close()])
      process.exit(0)
    } catch (e) {
      logSchema.error(logger, 'shutdown error', {
        type: 'SIGTERM',
        error: e,
      })
      process.exit(1)
    }
  })
})()
