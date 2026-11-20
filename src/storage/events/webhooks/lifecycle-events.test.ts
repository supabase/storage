import { vi } from 'vitest'

const {
  sendWebhook,
  sendWebhookBatch,
  purgeObjectCache,
  purgeObjectCacheBatch,
  logInfo,
  logError,
} = vi.hoisted(() => ({
  sendWebhook: vi.fn().mockResolvedValue(undefined),
  sendWebhookBatch: vi.fn().mockResolvedValue(undefined),
  purgeObjectCache: vi.fn().mockResolvedValue(undefined),
  purgeObjectCacheBatch: vi.fn().mockResolvedValue(undefined),
  logInfo: vi.fn(),
  logError: vi.fn(),
}))

vi.mock('./webhook', () => ({ sendWebhook, sendWebhookBatch }))
vi.mock('../cdn/purge-cdn-cache', () => ({ purgeObjectCache, purgeObjectCacheBatch }))
vi.mock('@internal/monitoring', () => ({ logger: { info: logInfo, error: logError } }))

import { ObjectRemoved } from './lifecycle-events'

/** `ObjectRemoved.sendWebhookBatch` wraps each `{ payload, applyTime }` with its eventType/
 * version before forwarding to the free `sendWebhookBatch` — match that wire shape, not the
 * caller-facing one. */
function wrapped(events: ReadonlyArray<{ payload: unknown; applyTime: number }>) {
  return events.map(({ payload, applyTime }) => ({
    eventType: 'ObjectRemoved:Delete',
    version: 'v1',
    payload,
    applyTime,
  }))
}

function webhookPayload() {
  return {
    tenant: { ref: 'tenant-a', host: 'tenant-a.example.test' },
    bucketId: 'bucket-a',
    name: 'path/file.png',
    version: 'version-1' as string | null,
    bytesChanged: -42,
    objectsChanged: -1,
    reqId: 'req-1',
    sbReqId: 'sb-req-1',
  }
}

describe('webhookLifecycleEvent.notify', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    sendWebhook.mockResolvedValue(undefined)
    purgeObjectCache.mockResolvedValue(undefined)
  })

  it('always sends the webhook, under its own eventType and version', async () => {
    await ObjectRemoved.notify(webhookPayload(), 1)

    expect(sendWebhook).toHaveBeenCalledExactlyOnceWith(
      'ObjectRemoved:Delete',
      'v1',
      webhookPayload(),
      1
    )
  })

  it('does not purge the object cache when purgeCache is not requested', async () => {
    await ObjectRemoved.notify(webhookPayload(), 1)

    expect(purgeObjectCache).not.toHaveBeenCalled()
  })

  it('purges the object cache alongside the webhook when purgeCache is requested', async () => {
    await ObjectRemoved.notify(webhookPayload(), 1, { purgeCache: true })

    expect(sendWebhook).toHaveBeenCalledOnce()
    expect(purgeObjectCache).toHaveBeenCalledExactlyOnceWith(webhookPayload())
  })

  it('still sends the webhook when the purge rejects, and vice versa', async () => {
    purgeObjectCache.mockRejectedValue(new Error('cdn unreachable'))

    await expect(ObjectRemoved.notify(webhookPayload(), 1, { purgeCache: true })).rejects.toThrow(
      'cdn unreachable'
    )
    expect(sendWebhook).toHaveBeenCalledOnce()
  })

  it('logs the billing event synchronously, before the webhook is sent', async () => {
    await ObjectRemoved.notify(webhookPayload(), 1)

    expect(logInfo).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        type: 'event',
        event: 'ObjectRemoved:Delete',
        objectPath: 'tenant-a/bucket-a/path/file.png',
        objectVersion: 'version-1',
        bytesChanged: -42,
        resources: ['/tenant-a/bucket-a/path/file.png'],
        tenantId: 'tenant-a',
        project: 'tenant-a',
        reqId: 'req-1',
        sbReqId: 'sb-req-1',
      }),
      '[Lifecycle]: ObjectRemoved:Delete tenant-a/bucket-a/path/file.png'
    )

    const [logOrder] = logInfo.mock.invocationCallOrder
    const [sendOrder] = sendWebhook.mock.invocationCallOrder
    expect(logOrder).toBeLessThan(sendOrder)
  })

  it('still logs the billing event exactly once when the webhook send rejects', async () => {
    sendWebhook.mockRejectedValue(new Error('queue unavailable'))

    await expect(ObjectRemoved.notify(webhookPayload(), 1)).rejects.toThrow('queue unavailable')

    expect(logInfo).toHaveBeenCalledOnce()
  })

  it('still sends the webhook, without throwing, when logging the billing event fails', async () => {
    logInfo.mockImplementation(() => {
      throw new Error('serialization failed')
    })

    await expect(ObjectRemoved.notify(webhookPayload(), 1)).resolves.toBeUndefined()

    expect(sendWebhook).toHaveBeenCalledOnce()
    expect(logError).toHaveBeenCalledWith(
      expect.objectContaining({
        error: expect.any(Error),
        tenantId: 'tenant-a',
        sbReqId: 'sb-req-1',
      }),
      'error logging lifecycle event: ObjectRemoved:Delete'
    )
  })
})

describe('webhookLifecycleEvent.notifyBatch', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    sendWebhookBatch.mockResolvedValue(undefined)
    purgeObjectCacheBatch.mockResolvedValue(undefined)
  })

  it('always sends the webhook batch', async () => {
    const events = [{ payload: webhookPayload(), applyTime: 1 }]

    await ObjectRemoved.notifyBatch(events)

    expect(sendWebhookBatch).toHaveBeenCalledExactlyOnceWith(wrapped(events), undefined)
  })

  it('does not purge when purgeCache is not requested', async () => {
    await ObjectRemoved.notifyBatch([{ payload: webhookPayload(), applyTime: 1 }])

    expect(purgeObjectCacheBatch).not.toHaveBeenCalled()
  })

  it('purges every object in the batch when purgeCache is requested', async () => {
    const events = [
      { payload: webhookPayload(), applyTime: 1 },
      { payload: { ...webhookPayload(), name: 'path/other.png' }, applyTime: 2 },
    ]

    await ObjectRemoved.notifyBatch(events, { purgeCache: true })

    expect(purgeObjectCacheBatch).toHaveBeenCalledExactlyOnceWith(
      events.map((event) => event.payload),
      undefined
    )
  })

  it('forwards the abort signal to both the webhook batch send and the purge batch send', async () => {
    const { signal } = new AbortController()
    const events = [{ payload: webhookPayload(), applyTime: 1 }]

    await ObjectRemoved.notifyBatch(events, { purgeCache: true, signal })

    expect(sendWebhookBatch).toHaveBeenCalledExactlyOnceWith(wrapped(events), { signal })
    expect(purgeObjectCacheBatch).toHaveBeenCalledExactlyOnceWith(
      events.map((event) => event.payload),
      { signal }
    )
  })

  it('logs every event, in order, before the batch is sent', async () => {
    const events = [
      { payload: webhookPayload(), applyTime: 1 },
      { payload: { ...webhookPayload(), name: 'path/other.png' }, applyTime: 2 },
    ]

    await ObjectRemoved.notifyBatch(events)

    expect(logInfo).toHaveBeenCalledTimes(2)
    expect(logInfo).toHaveBeenNthCalledWith(
      1,
      expect.anything(),
      '[Lifecycle]: ObjectRemoved:Delete tenant-a/bucket-a/path/file.png'
    )
    expect(logInfo).toHaveBeenNthCalledWith(
      2,
      expect.anything(),
      '[Lifecycle]: ObjectRemoved:Delete tenant-a/bucket-a/path/other.png'
    )

    const lastLog = Math.max(...logInfo.mock.invocationCallOrder)
    expect(lastLog).toBeLessThan(sendWebhookBatch.mock.invocationCallOrder[0])
  })
})
