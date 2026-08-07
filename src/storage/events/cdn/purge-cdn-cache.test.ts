import type { WirePayload } from '@internal/queue'
import type { JobContext } from '@supabase-labs/wave-core'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { PurgeCdnCachePayload } from './purge-cdn-cache'

// Minimal stand-in for `storageEvent`: enough class surface for TopicHandler, without
// pulling base.ts's storage/database import graph into the unit test.
vi.mock('../base', () => ({
  storageEvent: (opts: { type: string }) =>
    class {
      static readonly eventType = opts.type
      constructor(readonly data: unknown) {}
    },
}))

vi.mock('../topics', () => ({
  TOPICS: { purgeCdnCache: 'cdn-purge-cache' },
  backupRetry: (topic: string) => ({
    maxAttempts: 6,
    backoffMs: 5_000,
    deadLetter: `${topic}-dead-letter`,
  }),
}))

type CdnConfig = {
  cdnPurgeEndpointURL?: string
  cdnPurgeEndpointKey?: string
}

async function importPurgeCdnCache(config: CdnConfig = {}) {
  vi.resetModules()

  const configModule = await import('../../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig(config)

  return import('./purge-cdn-cache')
}

function makeCtx(): JobContext<WirePayload<PurgeCdnCachePayload>> {
  return {
    topic: 'cdn-purge-cache',
    group: 'cdn-purge-cache',
    message: {
      id: 'test-purge-cdn-cache',
      data: {
        tenant: { ref: 'tenant-ref', host: 'tenant-host' },
        sbReqId: 'sb-req-1',
        purgeOptions: {
          type: 'bucket',
          bucket: 'bucket-id',
          tenant: 'tenant-ref',
        },
        region: 'local',
      },
      headers: {},
      timestamp: 0,
      attempt: 1,
    },
    signal: new AbortController().signal,
    heartbeat: async () => {},
  }
}

describe('PurgeCdnCacheHandler.handle', () => {
  const cdnPurgeEndpointURL = 'https://cdn.example.com/stub/cache'
  const cdnPurgeEndpointKey = 'test-key'

  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
    vi.resetModules()
  })

  it('purges the CDN cache end-to-end when CDN_PURGE_ENDPOINT_URL is configured', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetchMock)

    const { PurgeCdnCacheHandler } = await importPurgeCdnCache({
      cdnPurgeEndpointURL,
      cdnPurgeEndpointKey,
    })

    await expect(new PurgeCdnCacheHandler().handle(makeCtx())).resolves.toBeUndefined()

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [input, init] = fetchMock.mock.calls[0]
    const requestInit = init as RequestInit
    expect(input.toString()).toBe(`${cdnPurgeEndpointURL}/purge`)
    expect((requestInit.headers as Headers).get('authorization')).toBe(
      `Bearer ${cdnPurgeEndpointKey}`
    )
    expect(JSON.parse(requestInit.body as string)).toEqual({
      type: 'bucket',
      tenant: { ref: 'tenant-ref' },
      bucketId: 'bucket-id',
    })
  })

  it('returns early without attempting a request when CDN_PURGE_ENDPOINT_URL is not set', async () => {
    const fetchMock = vi.fn<typeof fetch>()
    vi.stubGlobal('fetch', fetchMock)

    const { PurgeCdnCacheHandler } = await importPurgeCdnCache({
      cdnPurgeEndpointURL: undefined,
    })

    await expect(new PurgeCdnCacheHandler().handle(makeCtx())).resolves.toBeUndefined()

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('logs and rethrows when the CDN cache manager throws an unexpected error, so the queue retries the job', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockRejectedValue(new Error('socket hang up'))
    vi.stubGlobal('fetch', fetchMock)

    const { PurgeCdnCacheHandler } = await importPurgeCdnCache({
      cdnPurgeEndpointURL,
      cdnPurgeEndpointKey,
    })

    await expect(new PurgeCdnCacheHandler().handle(makeCtx())).rejects.toMatchObject({
      code: 'InternalError',
      message: 'Error purging cache',
      originalError: expect.objectContaining({
        message: 'socket hang up',
      }),
    })

    expect(fetchMock).toHaveBeenCalledTimes(1)
  })
})
