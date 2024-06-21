import { Queue } from './queue'
import { logger, logSchema } from './monitoring'
import adminApp from './admin-app'
import { getConfig } from './config'

export async function main() {
  const { requestTraceHeader, adminPort, host } = getConfig()

  logger.info('[Queue] Starting Queue Worker')
  const queue = await Queue.init()

  const server = adminApp({
    logger,
    disableRequestLogging: true,
    requestIdHeader: requestTraceHeader,
  })

  process.on('SIGTERM', async () => {
    logger.info('[Worker] Stopping')
    await server.close()
    await Queue.stop()
  })

  await server.listen({ port: adminPort, host })

  return new Promise<void>((resolve, reject) => {
    queue.on('error', (err) => {
      logger.info('[Queue] Error', err)
      reject(err)
    })

    queue.on('stopped', () => {
      logger.info('[Queue] Stopping')
      resolve()
    })
  })
}

process.on('uncaughtException', (e) => {
  logSchema.error(logger, 'uncaught exception', {
    type: 'uncaughtException',
    error: e,
  })
  logger.flush()
  process.exit(1)
})

main()
  .then(() => {
    logger.info('[Queue] Worker Exited Successfully')
  })
  .catch(() => {
    process.exit(1)
  })
