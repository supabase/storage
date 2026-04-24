import '@internal/monitoring/otel-tracing'
import '@internal/monitoring/otel-metrics'

import { IncomingMessage, Server, ServerResponse } from 'node:http'
import { Cluster } from '@internal/cluster/cluster'
import { AsyncAbortController } from '@internal/concurrency'
import {
  listenForTenantUpdate,
  multitenantKnex,
  PubSub,
  TenantConnection,
} from '@internal/database'
import {
  runMigrationsOnTenant,
  runMultitenantMigrations,
  startAsyncMigrations,
} from '@internal/database/migrations'
import { logger, logSchema } from '@internal/monitoring'
import { Queue, SYSTEM_TENANT } from '@internal/queue'
import { KnexShardStoreFactory, ShardCatalog } from '@internal/sharding'
import { getGlobal } from '@platformatic/globals'
import { registerWorkers } from '@storage/events'
import { SyncCatalogIds } from '@storage/events/upgrades/sync-catalog-ids'
import { FastifyInstance } from 'fastify'
import buildAdmin from '../admin-app'
import build from '../app'
import { getConfig } from '../config'
import { bindShutdownSignals, createServerClosedPromise, shutdown } from './shutdown'

const shutdownSignal = new AsyncAbortController()

bindShutdownSignals(shutdownSignal)
registerPlatformaticCloseHandler()

// Start API server
main()
  .then(() => {
    logSchema.info(logger, '[Server] Started Successfully', {
      type: 'server',
    })
  })
  .catch(async (e) => {
    logSchema.error(logger, 'Server not started with error', {
      type: 'startupError',
      error: e,
    })

    await close()
    process.exit(1)
  })
  .catch(() => {
    process.exit(1)
  })

/**
 * Start Storage API server
 */
async function main() {
  const {
    databaseURL,
    isMultitenant,
    pgQueueEnable,
    dbMigrationFreezeAt,
    vectorS3Buckets,
    icebergShards,
    numWorkers,
  } = getConfig()

  // Queue
  if (pgQueueEnable) {
    await Queue.start({
      signal: shutdownSignal.nextGroup.signal,
      registerWorkers,
    })

    logSchema.info(logger, '[Queue] Started', {
      type: 'queue',
    })
  }

  // Sharding for special buckets (vectors, analytics)
  const sharding = new ShardCatalog(new KnexShardStoreFactory(multitenantKnex))

  // Migrations
  if (isMultitenant) {
    await runMultitenantMigrations()
    await upgrades()
    await listenForTenantUpdate(PubSub)

    // Create shards for vector S3 buckets
    await sharding.createShards(
      vectorS3Buckets?.map((s) => ({
        shardKey: s,
        kind: 'vector',
        capacity: 10000,
        status: 'active',
      }))
    )

    // Create shards for analytics buckets
    await sharding.createShards(
      icebergShards.map((shard) => ({
        shardKey: shard,
        kind: 'iceberg-table',
        capacity: 10000,
        status: 'active',
      }))
    )
  } else {
    await runMigrationsOnTenant({
      databaseUrl: databaseURL,
      upToMigration: dbMigrationFreezeAt,
    })
  }

  // Pubsub
  await PubSub.start({
    signal: shutdownSignal.nextGroup.signal,
  })

  // Start async migrations background process
  if (isMultitenant && pgQueueEnable) {
    startAsyncMigrations(shutdownSignal.nextGroup.signal)
  }

  // PoolManager
  TenantConnection.poolManager.setNumWorkers(numWorkers)
  TenantConnection.poolManager.monitor()

  // Cluster information
  await Cluster.init(shutdownSignal.nextGroup.signal)

  Cluster.on('change', (data) => {
    logger.info(
      {
        type: 'cluster',
        clusterSize: data.size,
      },
      `[Cluster] Cluster size changed to ${data.size}`
    )
    TenantConnection.poolManager.rebalanceAll({
      clusterSize: data.size,
    })
  })

  // HTTP Server
  const app = await httpServer(shutdownSignal.signal)

  // HTTP Admin Server
  if (isMultitenant) {
    await httpAdminServer(app, shutdownSignal.signal)
  }
}

/**
 * Starts HTTP API Server
 * @param signal
 */
async function httpServer(signal: AbortSignal) {
  const { exposeDocs, requestTraceHeader, port, host } = getConfig()

  const app: FastifyInstance<Server, IncomingMessage, ServerResponse> = build({
    loggerInstance: logger,
    disableRequestLogging: true,
    exposeDocs,
    requestIdHeader: requestTraceHeader,
    routerOptions: { maxParamLength: 2500 },
  })

  const serverClosedPromise = createServerClosedPromise(app.server, () => {
    logSchema.info(logger, '[Server] Exited', {
      type: 'server',
    })
  })

  try {
    signal.addEventListener(
      'abort',
      async () => {
        logSchema.info(logger, '[Server] Stopping', {
          type: 'server',
        })

        await serverClosedPromise
      },
      { once: true }
    )
    await app.listen({ port, host, signal })

    return app
  } catch (err) {
    logSchema.error(logger, `Server failed to start`, {
      type: 'serverStartError',
      error: err,
    })
    throw err
  }
}

/**
 * Starts HTTP Admin endpoints
 * @param app
 * @param signal
 */
async function httpAdminServer(
  app: FastifyInstance<Server, IncomingMessage, ServerResponse>,
  signal: AbortSignal
) {
  const { exposeDocs, adminRequestIdHeader, adminPort, host } = getConfig()

  const adminApp = buildAdmin({
    loggerInstance: logger,
    disableRequestLogging: true,
    exposeDocs,
    requestIdHeader: adminRequestIdHeader,
  })

  const adminServerClosedPromise = createServerClosedPromise(adminApp.server, () => {
    logSchema.info(logger, '[Admin Server] Exited', {
      type: 'server',
    })
  })

  signal.addEventListener(
    'abort',
    async () => {
      logSchema.info(logger, '[Admin Server] Stopping', {
        type: 'server',
      })

      await adminServerClosedPromise
    },
    { once: true }
  )

  try {
    await adminApp.listen({ port: adminPort, host, signal })
  } catch (err) {
    logSchema.error(logger, 'Failed to start admin app', {
      type: 'adminAppStartError',
      error: err,
    })
    throw err
  }
  return adminApp
}

export async function close() {
  return shutdown(shutdownSignal)
}

function registerPlatformaticCloseHandler() {
  const platformatic = getGlobal()

  if (!platformatic?.events) {
    return
  }

  platformatic.events.on('close', () => {
    void close()
  })
}

async function upgrades() {
  return Promise.all([
    SyncCatalogIds.invoke({
      tenant: SYSTEM_TENANT,
    }),
  ])
}
