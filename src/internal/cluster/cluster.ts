import { EventEmitter } from 'node:events'
import { logger } from '@internal/monitoring'
import { getConfig } from '../../config'

const clusterEvent = new EventEmitter()

interface ClusterEvents {
  change: { size: number }
}

interface ClusterDiscovery {
  getClusterSize(signal: AbortSignal): Promise<number>
}

export class Cluster {
  static size: number = 0
  protected static watcher?: NodeJS.Timeout = undefined

  static on<E extends keyof ClusterEvents>(
    event: E,
    listener: (payload: ClusterEvents[E]) => void
  ) {
    clusterEvent.on(event, listener)
  }

  static async init(abortSignal: AbortSignal) {
    if (abortSignal.aborted) {
      return
    }

    let cluster: ClusterDiscovery | null = null
    const discoveryType = process.env.CLUSTER_DISCOVERY
    const config = getConfig()

    if (discoveryType === 'ECS') {
      const { ClusterDiscoveryECS } = await import('./ecs')

      if (abortSignal.aborted) {
        return
      }

      cluster = new ClusterDiscoveryECS()
    } else if (discoveryType === 'EKS') {
      const { ClusterDiscoveryEKS } = await import('./eks')

      if (abortSignal.aborted) {
        return
      }

      cluster = new ClusterDiscoveryEKS()
    }

    if (cluster) {
      const discover = async () => {
        const controller = new AbortController()
        const { promise, reject } = Promise.withResolvers<never>()
        const abort = (reason: unknown) => {
          reject(reason)
          controller.abort()
        }
        const onShutdown = () => abort(abortSignal.reason)
        abortSignal.addEventListener('abort', onShutdown)
        const timer = setTimeout(
          abort,
          config.clusterDiscoveryTimeoutMs,
          new DOMException('Cluster discovery timed out', 'TimeoutError')
        )

        try {
          return await Promise.race([cluster.getClusterSize(controller.signal), promise])
        } finally {
          clearTimeout(timer)
          abortSignal.removeEventListener('abort', onShutdown)
        }
      }

      const clusterSize = await discover().catch((error) => {
        if (!abortSignal.aborted) {
          throw error
        }
      })
      if (clusterSize === undefined || abortSignal.aborted) {
        return
      }

      Cluster.size = clusterSize

      logger.info(
        {
          type: 'cluster',
          clusterSize: Cluster.size,
          discoveryType,
        },
        `[Cluster] Initial cluster size ${Cluster.size}`
      )

      if (abortSignal.aborted) {
        return
      }

      let failures = 0
      const poll = async () => {
        try {
          const size = await discover()

          if (abortSignal.aborted) {
            return
          }
          failures = 0

          if (size && size !== Cluster.size) {
            Cluster.size = size
            try {
              clusterEvent.emit('change', { size })
            } catch (e) {
              console.error('Error notifying cluster size change', e)
            }
          }
        } catch (e) {
          if (abortSignal.aborted) {
            return
          }
          failures++
          console.error('Error getting cluster size', e)
        }

        if (!abortSignal.aborted) {
          schedule()
        }
      }

      const schedule = () => {
        let interval = config.clusterDiscoveryPollIntervalMs
        if (discoveryType === 'ECS') {
          // Every worker lists the whole family; ListTasks returns up to 100 ARNs per page.
          const tasks = Math.max(1, Cluster.size)
          const requests = tasks * Math.max(1, config.numWorkers) * Math.ceil(tasks / 100)
          interval = Math.max(interval, (requests * 1000) / config.clusterDiscoveryEcsMaxRps)
        }
        // Cap failure backoff at five minutes before jitter, preserving larger fleet intervals.
        const delay = Math.max(interval, Math.min(300_000, interval * 2 ** failures))
        Cluster.watcher = setTimeout(poll, Math.min(2 ** 31 - 1, delay * (0.5 + Math.random())))
      }

      schedule()

      abortSignal.addEventListener(
        'abort',
        () => {
          if (Cluster.watcher) {
            clearTimeout(Cluster.watcher)
            clusterEvent.removeAllListeners()
            Cluster.watcher = undefined
          }
        },
        { once: true }
      )
    }
  }
}
