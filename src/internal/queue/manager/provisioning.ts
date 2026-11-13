import type { ProducerAdapter } from './adapter'
import type { QueuesView } from './registry'
import { type RunOnce, runOnce } from './signals'
import { validateRegistry } from './validate'

/**
 * Startup: validate every declared queue's policy, run the adapter's `start()` hook, then (when `autoProvision`) provision every
 * declared queue with its declared options. A queue's dead-letter target is provisioned
 * BEFORE the queue (with its own declared options, or options-less when the registry does
 * not declare it), so an adapter can bind the two as it provisions. Memoised so concurrent
 * starts share one pass; a failed pass is not remembered, so the next call retries it.
 */
export function createProvisioning(
  adapter: Pick<ProducerAdapter, 'start' | 'provision'>,
  registry: QueuesView,
  autoProvision: boolean
): RunOnce<void> {
  return runOnce(async (): Promise<void> => {
    validateRegistry(registry)
    await adapter.start?.()
    if (!autoProvision) return
    // One provision per queue, each waiting on its dead-letter target's. `chain` is the
    // path of queues waiting on this one: meeting itself there is a dead-letter cycle.
    const provisioned = new Map<string, Promise<void>>()
    const provision = (name: string, chain: readonly string[]): Promise<void> => {
      if (chain.includes(name)) {
        throw new Error(`queue: dead-letter cycle — ${[...chain, name].join(' → ')}`)
      }
      let pending = provisioned.get(name)
      if (pending === undefined) {
        pending = (async () => {
          const options = registry.get(name)?.options
          const deadLetter = options?.retry?.deadLetter
          if (deadLetter !== undefined) await provision(deadLetter, [...chain, name])
          await adapter.provision(name, options)
        })()
        provisioned.set(name, pending)
      }
      return pending
    }
    await Promise.all(registry.names().map((name) => provision(name, [])))
  })
}
