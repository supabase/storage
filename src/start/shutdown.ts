import { logger, logSchema } from '@internal/monitoring'
import { AsyncAbortController } from '@internal/concurrency'
import { multitenantKnex, TenantConnection } from '@internal/database'
import http from 'http'

/**
 * Binds shutdown handlers to the process
 * @param serverSignal
 */
export function bindShutdownSignals(serverSignal: AsyncAbortController) {
  // Register handlers
  process.on('uncaughtException', (e) => {
    logSchema.error(logger, 'uncaught exception', {
      type: 'uncaughtException',
      error: e,
    })
    process.exit(1)
  })

  // Shutdown handler
  process.on('SIGTERM', async () => {
    logSchema.info(logger, '[Server] Received SIGTERM, shutting down', {
      type: 'shutdown',
    })
    try {
      await shutdown(serverSignal)
      logSchema.info(logger, '[Server] SIGTERM Shutdown successfully', {
        type: 'shutdown',
      })
    } catch (e) {
      logSchema.error(logger, '[Server] SIGTERM Shutdown with error', {
        type: 'shutdown',
        error: e,
      })
      process.exit(1)
    }
  })
}

/**
 * Gracefully shuts down the server
 * @param serverSignal
 */
export async function shutdown(serverSignal: AsyncAbortController) {
  try {
    const errors: unknown[] = []

    await serverSignal.abortAsync().catch((e) => {
      logSchema.error(logger, 'Failed to abort server signal', {
        type: 'shutdown',
        error: e,
      })
      errors.push(e)
    })

    await multitenantKnex.destroy().catch((e) => {
      logSchema.error(logger, 'Failed to close database connection', {
        type: 'shutdown',
        error: e,
      })
      errors.push(e)
    })

    await TenantConnection.stop().catch((e) => {
      logSchema.error(logger, 'Failed to close tenant connection', {
        type: 'shutdown',
        error: e,
      })
    })

    if (errors.length > 0) {
      throw errors[errors.length - 1]
    }
  } catch (e) {
    logSchema.error(logger, 'shutdown error', {
      type: 'shutdown',
      error: e,
    })
    throw e
  } finally {
    logger.flush()
  }
}

export function createServerClosedPromise(server: http.Server, cb: () => Promise<void> | void) {
  return new Promise<void>((res) => {
    server.once('close', async () => {
      await cb()
      res()
    })
  })
}
