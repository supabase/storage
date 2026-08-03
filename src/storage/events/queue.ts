import { getWave, type StorageWave, startQueue } from '@internal/queue'
import { closeStorageBackend } from './base'
import { type StorageTopics, storageTopics } from './topics'
import { buildHandlers } from './workers'

/** Start the storage queue (idempotent) — `@internal/queue`'s `startQueue` with
 * `storageTopics`/`buildHandlers`/`closeStorageBackend` already applied, so callers never need
 * to name `StorageTopics` themselves. */
export function startStorageQueue(opts: { signal?: AbortSignal } = {}) {
  return startQueue<StorageTopics>(
    { topics: storageTopics, handlers: buildHandlers(), onStop: closeStorageBackend },
    opts
  )
}

/** The typed storage wave — `@internal/queue`'s `getWave<StorageTopics>()` with the type
 * argument already applied, so every call site (including handler files) can produce/invoke
 * without naming `StorageTopics` themselves. */
export function getStorageQueue(): StorageWave<StorageTopics> {
  return getWave<StorageTopics>()
}
