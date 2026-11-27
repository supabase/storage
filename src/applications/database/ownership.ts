import { workerData } from 'node:worker_threads'

/** The worker of the Database application this thread is, as Watt describes it. */
export type DatabaseWorkerIdentity = {
  /** Watt's worker ID, `<application>:<index>`: the ID messaging affinity hashes on. */
  id: string
  /** How many workers the application is configured to run. */
  count: number
}

/**
 * Which destinations this Database worker may serve.
 *
 * Storage workers send every message for a destination with its ID as the messaging
 * affinity key, and Watt picks the worker by rendezvous hashing over the workers that are
 * alive. While a worker restarts, its destinations would hash to the others; a transaction
 * opened there would be orphaned once the worker is back and the commit hashes to it again.
 * So a worker only opens pools and transactions for the destinations it owns among all the
 * configured workers, alive or not, and rejects the others with a retryable error: during a
 * restart the restarting worker's destinations fail fast instead of moving.
 */
export class DestinationOwnership {
  private readonly workerIds: string[]

  constructor(private readonly self: DatabaseWorkerIdentity | undefined) {
    const separator = self ? self.id.lastIndexOf(':') : -1
    this.workerIds =
      self && self.count > 1 && separator > 0
        ? Array.from({ length: self.count }, (_, i) => `${self.id.slice(0, separator)}:${i}`)
        : []
  }

  /** The worker ID that owns `destinationId`, or undefined when there is only one worker. */
  ownerOf(destinationId: string): string | undefined {
    return this.workerIds.length > 1 ? pickByAffinity(this.workerIds, destinationId) : undefined
  }

  owns(destinationId: string): boolean {
    const owner = this.ownerOf(destinationId)
    return owner === undefined || owner === this.self?.id
  }
}

export function currentDatabaseWorker(): DatabaseWorkerIdentity | undefined {
  const worker = (workerData as { worker?: { id?: unknown; count?: unknown } } | null)?.worker
  if (typeof worker?.id !== 'string' || typeof worker.count !== 'number') {
    return undefined
  }
  return { id: worker.id, count: worker.count }
}

// The rendezvous hashing of Platformatic messaging (@platformatic/runtime,
// lib/worker/affinity.js), which it uses to route a message sent with an affinity key.
// ownership.test.ts checks the two agree, so a change on Platformatic's side fails the tests.
export function pickByAffinity(workerIds: string[], key: string): string | undefined {
  let selected: string | undefined
  let selectedScore = -1

  for (const id of workerIds) {
    const score = hash(`${key}\0${id}`)
    if (
      score > selectedScore ||
      (score === selectedScore && selected !== undefined && id < selected)
    ) {
      selected = id
      selectedScore = score
    }
  }

  return selected
}

// 32-bit FNV-1a, then the MurmurHash3 finalizer.
function hash(value: string): number {
  let h = 0x811c9dc5
  for (let i = 0; i < value.length; i++) {
    h ^= value.charCodeAt(i)
    h = Math.imul(h, 0x01000193)
  }

  h ^= h >>> 16
  h = Math.imul(h, 0x85ebca6b)
  h ^= h >>> 13
  h = Math.imul(h, 0xc2b2ae35)
  h ^= h >>> 16

  return h >>> 0
}
