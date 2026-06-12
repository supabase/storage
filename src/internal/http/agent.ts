import {
  httpPoolBusySockets,
  httpPoolErrors,
  httpPoolFreeSockets,
  httpPoolPendingRequests,
} from '@internal/monitoring/metrics'
import { Agent } from 'undici'
import { getConfig } from '../../config'

const { region } = getConfig()

export interface InstrumentedAgent {
  dispatcher: Agent
  monitor: () => NodeJS.Timeout | undefined
  close: () => Promise<void>
}

export interface AgentStats {
  busySocketCount: number
  freeSocketCount: number
  pendingRequestCount: number
  errorSocketCount: number
}

export interface AgentOptions {
  maxSockets: number
  connectTimeoutMs?: number
  requestTimeoutMs?: number
}

/**
 * Creates an instrumented undici Agent.
 * Tracks connect errors via the Agent's `connectionError` event so the
 * `http_pool_errors` metric stays populated.
 */
export function createAgent(name: string, options: AgentOptions): InstrumentedAgent {
  const dispatcher = new Agent({
    connections: options.maxSockets,
    keepAliveTimeout: 15_000,
    pipelining: 1,
    headersTimeout: options.requestTimeoutMs ?? 0,
    bodyTimeout: options.requestTimeoutMs ?? 0,
    connect: {
      timeout: options.connectTimeoutMs ?? 5_000,
      keepAlive: true,
      keepAliveInitialDelay: 1_000,
    },
  })

  let errorCount = 0
  dispatcher.on('connectionError', () => {
    errorCount++
  })

  let watcher: NodeJS.Timeout | undefined
  let closing: Promise<void> | undefined

  return {
    dispatcher,
    monitor: () => {
      watcher = watchAgent(name, dispatcher, () => errorCount)
      return watcher
    },
    close: () => {
      if (closing) return closing
      if (watcher) {
        clearInterval(watcher)
        watcher = undefined
      }
      closing = dispatcher.close()
      return closing
    },
  }
}

function updateHttpAgentMetrics(name: string, stats: AgentStats) {
  const baseAttrs = { name, protocol: 'https' }

  httpPoolBusySockets.record(stats.busySocketCount, baseAttrs)
  httpPoolFreeSockets.record(stats.freeSocketCount, baseAttrs)
  httpPoolPendingRequests.record(stats.pendingRequestCount, { name, region })
  httpPoolErrors.record(stats.errorSocketCount, { ...baseAttrs, type: 'connect_error' })
}

export function watchAgent(name: string, dispatcher: Agent, getErrorCount: () => number) {
  return setInterval(() => {
    const stats = gatherDispatcherStats(dispatcher, getErrorCount())
    updateHttpAgentMetrics(name, stats)
  }, 5000)
}

export function gatherDispatcherStats(dispatcher: Agent, errorCount: number): AgentStats {
  let busySocketCount = 0
  let freeSocketCount = 0
  let pendingRequestCount = 0

  for (const origin of Object.keys(dispatcher.stats)) {
    const s = dispatcher.stats[origin] as {
      running: number
      free?: number
      pending: number
      queued?: number
    }
    busySocketCount += s.running
    freeSocketCount += s.free ?? 0
    pendingRequestCount += s.pending + (s.queued ?? 0)
  }

  return {
    busySocketCount,
    freeSocketCount,
    pendingRequestCount,
    errorSocketCount: errorCount,
  }
}
