import { AsyncAbortController } from '@internal/concurrency'
import { multitenantKnex, TenantConnection } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
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
  let isShuttingDown = false
  const gracefulShutdown = async (signal: string) => {
    if (isShuttingDown) {
      logSchema.info(logger, `[Server] Received ${signal} again, forcing exit`, {
        type: 'shutdown',
      })
      process.exit(1)
    }
    isShuttingDown = true

    logSchema.info(logger, `[Server] Received ${signal}, shutting down`, {
      type: 'shutdown',
    })
    try {
      await shutdown(serverSignal)
      logSchema.info(logger, `[Server] ${signal} Shutdown successfully`, {
        type: 'shutdown',
      })
      process.exit(0)
    } catch (e) {
      logSchema.error(logger, `[Server] ${signal} Shutdown with error`, {
        type: 'shutdown',
        error: e,
      })
      process.exit(1)
    }
  }

  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'))
  process.on('SIGINT', () => gracefulShutdown('SIGINT'))
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
