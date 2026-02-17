import { Queue } from '@internal/queue'
import { EventNotifier } from '@internal/queue/event-notifier'
import { EventPublisher } from '@internal/queue/event-publisher'
import { logger, logSchema, setLogger } from '@internal/monitoring'
import { listenForTenantUpdate, multitenantKnex, PubSub } from '@internal/database'
import { AsyncAbortController } from '@internal/concurrency'
import { getConfig } from '../config'
import adminApp from '../admin-app'
import { bindShutdownSignals, createServerClosedPromise, shutdown } from './shutdown'

const publisherLogger = logger.child({ service: 'publisher' })
setLogger(publisherLogger)

const shutdownSignal = new AsyncAbortController()

bindShutdownSignals(shutdownSignal)

main()
  .then(() => {
    logSchema.info(logger, '[Publisher] Started successfully', {
      type: 'publisher',
    })
  })
  .catch(async (e) => {
    logSchema.error(logger, '[Publisher] Error starting', {
      type: 'publisher',
      error: e,
    })
    await shutdown(shutdownSignal)
    process.exit(1)
  })
  .catch(() => {
    process.exit(1)
  })

export async function main() {
  const { requestTraceHeader, adminPort, host, isMultitenant } = getConfig()

  logger.info('[Publisher] Starting Event Log Publisher')

  if (isMultitenant) {
    await listenForTenantUpdate(PubSub)
  }

  // Start Queue (pg-boss) for sending events only, no workers
  await Queue.start({
    signal: shutdownSignal.signal,
  })

  // Initialize event log notifier for tenant discovery
  if (isMultitenant) {
    EventNotifier.init(multitenantKnex)
  }

  // Start the event log processor
  await startEventLogProcessor(multitenantKnex, shutdownSignal.signal)

  if (isMultitenant) {
    await PubSub.start({
      signal: shutdownSignal.nextGroup.nextGroup.signal,
    })
  }

  const server = adminApp({
    logger,
    disableRequestLogging: true,
    requestIdHeader: requestTraceHeader,
  })

  const shutdownPromise = createServerClosedPromise(server.server, () => {
    logSchema.info(logger, '[Publisher Admin Server] Exited', {
      type: 'server',
    })
  })

  shutdownSignal.nextGroup.signal.addEventListener('abort', async () => {
    logSchema.info(logger, '[Publisher Admin Server] Stopping', {
      type: 'server',
    })
    await shutdownPromise
  })

  await server.listen({ port: adminPort, host, signal: shutdownSignal.nextGroup.signal })
}

/**
 * Starts the event log processor. Can be called from server.ts or publisher.ts.
 */
export async function startEventLogProcessor(
  multitenantKnex: import('knex').Knex,
  signal: AbortSignal
) {
  const processor = new EventPublisher(multitenantKnex)
  await processor.start(signal)
  return processor
}
