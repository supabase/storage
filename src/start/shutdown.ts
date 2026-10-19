import { AsyncAbortController } from '@internal/concurrency'
import { PgTenantConnection, shutdownMultitenantPg } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import http from 'http'
import { closeStorageBackend } from '../http/plugins/storage'

let shutdownPromise: Promise<void> | undefined
const shutdownPhaseTimeoutMs = 60_000
// Cooperative waits inside a phase stop this early so the phase can still finish cleanly.
const shutdownPhaseMarginMs = 1_000
const storageBackendCloseGraceMs = 2_000

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
  if (shutdownPromise) {
    return shutdownPromise
  }

  shutdownPromise = (async () => {
    try {
      const errors: unknown[] = []

      await runShutdownPhase('abort server signal', (deadline) =>
        serverSignal.abortAsync(deadline)
      ).catch((e) => {
        logSchema.error(logger, 'Failed to abort server signal', {
          type: 'shutdown',
          error: e,
        })
        errors.push(e)
      })

      if (errors.length === 0) {
        // HTTP has stopped; in-flight S3 calls belong to handlers whose clients left.
        const closed = closeStorageBackend().catch((e) => {
          logSchema.error(logger, 'Failed to close storage backend', {
            type: 'shutdown',
            error: e,
          })
          errors.push(e)
        })
        const expired = Promise.withResolvers<boolean>()
        const grace = setTimeout(expired.resolve, storageBackendCloseGraceMs, false).unref()
        const graceful = await Promise.race([closed.then(() => true), expired.promise])
        clearTimeout(grace)
        if (!graceful) {
          logSchema.info(logger, 'Storage backend close still pending after grace period', {
            type: 'shutdown',
          })
        }
      }

      await runShutdownPhase('close pg tenant connection', () => PgTenantConnection.stop()).catch(
        (e) => {
          logSchema.error(logger, 'Failed to close pg tenant connection', {
            type: 'shutdown',
            error: e,
          })
          errors.push(e)
        }
      )

      await runShutdownPhase('close pg database connection', shutdownMultitenantPg).catch((e) => {
        logSchema.error(logger, 'Failed to shutdown pg database connection', {
          type: 'shutdown',
          error: e,
        })
        errors.push(e)
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
  })()

  return shutdownPromise
}

export function createServerClosedPromise(server: http.Server, cb: () => Promise<void> | void) {
  return new Promise<void>((res) => {
    server.once('close', async () => {
      await cb()
      res()
    })
  })
}

async function runShutdownPhase<T>(name: string, phase: (deadline: number) => Promise<T>) {
  const deadline = Date.now() + shutdownPhaseTimeoutMs - shutdownPhaseMarginMs
  let timeout: ReturnType<typeof setTimeout> | undefined

  const timeoutPromise = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      reject(new Error(`Shutdown phase "${name}" timed out after ${shutdownPhaseTimeoutMs}ms`))
    }, shutdownPhaseTimeoutMs)
    timeout.unref?.()
  })

  try {
    return await Promise.race([phase(deadline), timeoutPromise])
  } finally {
    if (timeout) {
      clearTimeout(timeout)
    }
  }
}
