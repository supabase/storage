import { type Counter, metrics } from '@opentelemetry/api'
import type { BusyReason } from './errors.js'
import type { PoolRegistry } from './pools.js'

let registered = false
let rejectedRequests: Counter | undefined

/** Counts a request refused with BUSY, by the limit that refused it. */
export function countBusyRejection(reason: BusyReason): void {
  // Created on first use, once the process has registered its meter provider.
  rejectedRequests ??= metrics
    .getMeter('storage-database-watt')
    .createCounter('database_watt_rejected_requests', {
      description: 'Requests the Database Watt application refused with BUSY, by reason',
    })
  rejectedRequests.add(1, { reason })
}

export function registerDatabaseWattMetrics(pools: PoolRegistry): void {
  if (registered) {
    return
  }

  registered = true
  const meter = metrics.getMeter('storage-database-watt')
  const activePools = meter.createObservableGauge('database_watt_active_pools', {
    description: 'Current number of active Database Watt destination pools',
  })
  const totalConnections = meter.createObservableGauge('database_watt_connections', {
    description: 'Current number of Database Watt PostgreSQL connections',
  })
  const inUseConnections = meter.createObservableGauge('database_watt_connections_in_use', {
    description: 'Current number of Database Watt PostgreSQL connections in use',
  })
  const waitingRequests = meter.createObservableGauge('database_watt_waiting_requests', {
    description: 'Current number of Database Watt requests waiting for PostgreSQL connections',
  })

  meter.addBatchObservableCallback(
    (observer) => {
      const stats = pools.getStats()
      observer.observe(activePools, stats.pools)
      observer.observe(totalConnections, stats.totalConnections)
      observer.observe(inUseConnections, stats.inUseConnections)
      observer.observe(waitingRequests, stats.waitingRequests)
    },
    [activePools, totalConnections, inUseConnections, waitingRequests]
  )
}
