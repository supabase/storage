import { getQueue, type StorageQueue, startQueue } from '@internal/queue'
import { closeStorageBackend } from './base'
import { type StorageQueues, storageQueues } from './registry'
import { buildHandlers } from './workers'

/** Start the storage queue (idempotent): `@internal/queue`'s `startQueue` with the storage
 * registry, handlers and backend teardown already applied. */
export function startStorageQueue(opts: { signal?: AbortSignal } = {}) {
  return startQueue<StorageQueues>(
    { queues: storageQueues, handlers: buildHandlers(), onStop: closeStorageBackend },
    opts
  )
}

/** The typed storage queue: `getQueue<StorageQueues>()` with the type argument applied, so
 * call sites produce/invoke without naming `StorageQueues` themselves. */
export function getStorageQueue(): StorageQueue<StorageQueues> {
  return getQueue<StorageQueues>()
}
