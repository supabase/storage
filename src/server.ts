import './internal/monitoring/otel'
import { FastifyInstance } from 'fastify'
import { IncomingMessage, Server, ServerResponse } from 'http'

import build from './app'
import buildAdmin from './admin-app'
import { getConfig } from './config'
import {
  runMultitenantMigrations,
  runMigrationsOnTenant,
  startAsyncMigrations,
  TenantConnection,
  listenForTenantUpdate,
  PubSub,
  multitenantKnex,
} from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { Queue } from '@internal/queue'
import { registerWorkers } from '@storage/events'

const serverSignal = new AbortController()

process.on('uncaughtException', (e) => {
  logSchema.error(logger, 'uncaught exception', {
    type: 'uncaughtException',
    error: e,
  })
  process.exit(1)
})

// Start API server
main()
  .then(() => {
    logger.info('[Server] Started Successfully')
  })
  .catch((e) => {
    logSchema.error(logger, 'Server shutdown with error', {
      type: 'startupError',
      error: e,
    })
  })

/**
 * Start Storage API server
 */
async function main() {
  const {
    databaseURL,
    isMultitenant,
    requestTraceHeader,
    adminRequestIdHeader,
    adminPort,
    port,
    host,
    pgQueueEnable,
    pgQueueEnableWorkers,
    exposeDocs,
  } = getConfig()

  // Migrations
  if (isMultitenant) {
    await runMultitenantMigrations()
    await listenForTenantUpdate(PubSub)
    startAsyncMigrations(serverSignal.signal)
  } else {
    await runMigrationsOnTenant(databaseURL)
  }

  // Queue
  if (pgQueueEnable) {
    if (pgQueueEnableWorkers) {
      registerWorkers()
    }
    await Queue.init()
  }

  // Pubsub
  await PubSub.connect()

  // HTTP Server
  const app: FastifyInstance<Server, IncomingMessage, ServerResponse> = build({
    logger,
    disableRequestLogging: true,
    exposeDocs,
    requestIdHeader: requestTraceHeader,
  })

  app.listen({ port, host }, (err) => {
    if (err) {
      logSchema.error(logger, `Server failed to start`, {
        type: 'serverStartError',
        error: err,
      })
      process.exit(1)
    }
  })

  // HTTP Server Admin
  let adminApp: FastifyInstance<Server, IncomingMessage, ServerResponse> | undefined = undefined

  if (isMultitenant) {
    adminApp = buildAdmin(
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

  process.on('SIGTERM', async () => {
    try {
      logger.info('Received SIGTERM, shutting down')
      await Promise.allSettled([app.close(), adminApp?.close()])
      await Promise.allSettled([
        serverSignal.abort(),
        Queue.stop(),
        TenantConnection.stop(),
        PubSub.close(),
        multitenantKnex.destroy(),
      ])

      if (process.env.NODE_ENV !== 'production') {
        process.exit(0)
      }
    } catch (e) {
      logSchema.error(logger, 'shutdown error', {
        type: 'SIGTERM',
        error: e,
      })
      process.exit(1)
    }
  })
}
