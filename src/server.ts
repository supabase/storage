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
} from './database'
import { logger, logSchema } from './monitoring'
import { Queue } from './queue'

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

  const serverSignal = new AbortController()

  if (isMultitenant) {
    await runMultitenantMigrations()
    await listenForTenantUpdate(PubSub)
    startAsyncMigrations(serverSignal.signal)
  } else {
    await runMigrationsOnTenant(databaseURL)
  }

  if (pgQueueEnable) {
    await Queue.init()
  }

  let adminApp: FastifyInstance<Server, IncomingMessage, ServerResponse> | undefined = undefined
  const app: FastifyInstance<Server, IncomingMessage, ServerResponse> = build({
    logger,
    disableRequestLogging: true,
    exposeDocs,
    requestIdHeader: requestTraceHeader,
  })

  await PubSub.connect()

  app.listen({ port, host }, (err) => {
    if (err) {
      logSchema.error(logger, `Server failed to start`, {
        type: 'serverStartError',
        error: err,
      })
      process.exit(1)
    }
  })

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

      if (process.env.NODE_ENV !== 'production') {
        process.exit(1)
      }
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
      logger.info('Received SIGTERM, shutting down')
      await Promise.all([app.close(), adminApp?.close()])
      await Promise.allSettled([
        serverSignal.abort(),
        Queue.stop(),
        TenantConnection.stop(),
        PubSub.close(),
        multitenantKnex.destroy(),
      ])
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
