import type { PurgeCdnCachePayload } from '@storage/events/cdn/purge-cdn-cache'
import { type Mock, vi } from 'vitest'

interface PurgeCdnCacheClass {
  send(payload: PurgeCdnCachePayload): Promise<void>
  batchSend(
    payloads: ReadonlyArray<PurgeCdnCachePayload>,
    options?: { readonly signal?: AbortSignal }
  ): Promise<void>
}

export type PurgeSpy = Mock<(payload: PurgeCdnCachePayload) => void> & {
  /** Stop the purges from reaching the queue; they are still recorded. */
  stub(): PurgeSpy
}

/**
 * Observe the CDN purges `purgeObjectCache`/`purgeObjectCacheBatch`
 * for both one at a time (`PurgeCdnCache.send`) or as a batch (`PurgeCdnCache.batchSend`)
 */
export function spyOnPurge(PurgeCdnCache: PurgeCdnCacheClass): PurgeSpy {
  const sendOne = PurgeCdnCache.send.bind(PurgeCdnCache)
  const sendMany = PurgeCdnCache.batchSend.bind(PurgeCdnCache)
  let stubbed = false

  const seen = vi.fn<(payload: PurgeCdnCachePayload) => void>()
  vi.spyOn(PurgeCdnCache, 'send').mockImplementation(async (payload) => {
    seen(payload)
    if (!stubbed) await sendOne(payload)
  })
  vi.spyOn(PurgeCdnCache, 'batchSend').mockImplementation(async (payloads, options) => {
    for (const payload of payloads) seen(payload)
    if (!stubbed) await sendMany(payloads, options)
  })

  return Object.assign(seen, {
    stub() {
      stubbed = true
      return seen as PurgeSpy
    },
  })
}
