import type { ProducerAdapter } from './adapter'
import type { QueuesView } from './registry'
import { type RunOnce, runOnce } from './signals'

/**
 * Startup: run the adapter's `start()` hook, then (when `autoProvision`) provision every
 * declared queue. Memoised so concurrent starts share one pass; a failed pass is not
 * remembered, so the next call retries it.
 */
export function createProvisioning(
  adapter: Pick<ProducerAdapter, 'start' | 'provision'>,
  registry: QueuesView,
  autoProvision: boolean
): RunOnce<void> {
  return runOnce(async (): Promise<void> => {
    await adapter.start?.()
    if (!autoProvision) return
    await Promise.all(
      registry.names().map((name) => adapter.provision(name, registry.get(name)?.options))
    )
  })
}
