import { ClusterDiscoveryECS } from '@internal/cluster/ecs'

import { EventEmitter } from 'node:events'
import { logger } from '@internal/monitoring'

const clusterEvent = new EventEmitter()

export class Cluster {
  static size: number = 0
  protected static watcher?: NodeJS.Timeout = undefined

  static on(event: string, listener: (...args: any[]) => void) {
    clusterEvent.on(event, listener)
  }

  static async init(abortSignal: AbortSignal) {
    if (process.env.CLUSTER_DISCOVERY === 'ECS') {
      const cluster = new ClusterDiscoveryECS()
      Cluster.size = await cluster.getClusterSize()

      logger.info(`[Cluster] Initial cluster size ${Cluster.size}`, {
        type: 'cluster',
        clusterSize: Cluster.size,
      })

      Cluster.watcher = setInterval(() => {
        cluster
          .getClusterSize()
          .then((size) => {
            if (size && size !== Cluster.size) {
              clusterEvent.emit('change', { size })
              Cluster.size = size
            }
          })
          .catch((e) => {
            console.error('Error getting cluster size', e)
          })
      }, 20 * 1000)

      abortSignal.addEventListener(
        'abort',
        () => {
          if (Cluster.watcher) {
            clearInterval(Cluster.watcher)
            clusterEvent.removeAllListeners()
            Cluster.watcher = undefined
          }
        },
        { once: true }
      )
    }
  }
}
