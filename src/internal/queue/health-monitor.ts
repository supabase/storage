import { logger, logSchema } from '../monitoring'
import { Queue } from './queue'

export interface QueueHealthConfig {
  maxConsecutiveErrors: number
  unhealthyTimeoutMs: number
}

interface ConnectionError {
  code?: string
  message?: string
}

export class QueueHealthMonitor {
  private consecutiveConnectionErrors = 0
  private lastSuccessfulOperation = Date.now()
  private lastError = 0
  private shutdownStarted = false

  constructor(private config: QueueHealthConfig) {}

  trackConnectionError(error: ConnectionError): void {
    const isConnectionError = error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT'
    if (!isConnectionError) {
      return
    }

    this.consecutiveConnectionErrors++
    this.lastError = Date.now()

    logSchema.warning(logger, '[Queue Health] Connection error detected', {
      type: 'queue',
      metadata: JSON.stringify({
        consecutiveErrors: this.consecutiveConnectionErrors,
        timeSinceLastSuccess: Date.now() - this.lastSuccessfulOperation,
        errorCode: error.code,
        errorMessage: error.message,
      }),
    })

    this.checkHealth()
  }

  trackSuccessfulOperation(): void {
    if (this.shutdownStarted) {
      return
    }

    if (this.consecutiveConnectionErrors > 0) {
      logSchema.info(logger, '[Queue Health] Connection recovered', {
        type: 'queue',
        metadata: JSON.stringify({
          previousConsecutiveErrors: this.consecutiveConnectionErrors,
          downtime: Math.floor((Date.now() - this.lastError) / 1000),
        }),
      })
    }
    this.consecutiveConnectionErrors = 0
    this.lastSuccessfulOperation = Date.now()
  }

  private checkHealth(): void {
    const unhealthyDuration = Date.now() - this.lastSuccessfulOperation

    if (
      this.consecutiveConnectionErrors >= this.config.maxConsecutiveErrors ||
      unhealthyDuration >= this.config.unhealthyTimeoutMs
    ) {
      logSchema.error(
        logger,
        '[Queue Health] FATAL - Queue is unhealthy, initiating graceful shutdown',
        {
          type: 'queue',
          metadata: JSON.stringify({
            consecutiveErrors: this.consecutiveConnectionErrors,
            unhealthyDurationSeconds: Math.floor(unhealthyDuration / 1000),
            maxConsecutiveErrors: this.config.maxConsecutiveErrors,
            unhealthyTimeoutMs: this.config.unhealthyTimeoutMs,
          }),
        }
      )

      void this.initiateGracefulShutdown()
    }
  }

  private async initiateGracefulShutdown(): Promise<void> {
    if (this.shutdownStarted) {
      return
    }
    this.shutdownStarted = true

    logSchema.info(logger, '[Queue Health] Stopping queue to allow in-flight jobs to complete', {
      type: 'queue',
    })

    // Set a timeout for the stop operation in case it hangs
    // pg-boss.stop() has a 20 second timeout, so we wait 30 seconds total
    const stopTimeout = new Promise<void>((resolve) => {
      setTimeout(() => {
        logSchema.warning(logger, '[Queue Health] Stop operation timed out after 30 seconds', {
          type: 'queue',
        })
        resolve()
      }, 30000)
    })

    try {
      // Race between actual stop and timeout
      await Promise.race([Queue.stop(), stopTimeout])

      logSchema.error(logger, '[Queue Health] Queue stopped - worker must restart', {
        type: 'queue',
      })
    } catch (error) {
      logSchema.error(logger, '[Queue Health] Error during graceful shutdown', {
        type: 'queue',
        error,
      })
    }

    // Emit uncaughtException to trigger worker shutdown
    // This is caught by bindShutdownSignals() in shutdown.ts
    const fatalError = new Error('Queue health check failed - database connection unavailable')
    process.nextTick(() => {
      process.emit('uncaughtException', fatalError)
    })
  }
}
