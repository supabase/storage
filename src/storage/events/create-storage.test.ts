import { vi } from 'vitest'
import type { LifecyclePayload } from './webhooks/webhook'

const {
  connection,
  constructDatabase,
  constructStorage,
  createAgent,
  createStorageBackend,
  getPostgresConnection,
  getServiceKeyUser,
  logError,
  logInfo,
  produce,
} = vi.hoisted(() => ({
  connection: { dispose: vi.fn() },
  constructDatabase: vi.fn(),
  constructStorage: vi.fn(),
  createAgent: vi.fn(),
  createStorageBackend: vi.fn(),
  getPostgresConnection: vi.fn(),
  getServiceKeyUser: vi.fn(),
  logError: vi.fn(),
  logInfo: vi.fn(),
  produce: vi.fn(),
}))

// `sendWebhook`'s producing side: the queue `Webhook.send` produces on (read by base.ts from
// `@internal/queue`) and the queue names it reads.
vi.mock('@internal/queue', () => ({
  getQueue: () => ({ produce }),
}))

vi.mock('./registry', () => ({
  QUEUES: { webhooks: 'webhooks' },
  defaultRetry: (name: string) => ({
    maxAttempts: 21,
    backoffMs: 5_000,
    deadLetter: `${name}-dead-letter`,
  }),
}))

vi.mock('@internal/database', () => ({
  getPostgresConnection,
  getServiceKeyUser,
}))

vi.mock('@internal/http', () => ({
  createAgent,
}))

vi.mock('@internal/monitoring', () => ({
  logger: { error: logError, info: logInfo },
}))

vi.mock('../backend', () => ({
  createStorageBackend,
}))

vi.mock('../database', () => ({
  StoragePgDB: class {
    constructor(...args: unknown[]) {
      constructDatabase(...args)
    }
  },
}))

vi.mock('../storage', () => ({
  Storage: class {
    constructor(...args: unknown[]) {
      constructStorage(...args)
    }
  },
}))

vi.mock('../../config', () => ({
  getConfig: () => ({
    region: 'local',
    storageBackendType: 's3',
    storageS3Bucket: 'test-storage',
    storageS3MaxSockets: 10,
  }),
}))

async function createStorageForTest() {
  const { createStorage } = await import('./base')

  return createStorage({
    tenant: { ref: 'tenant-a', host: 'tenant-a.example.test' },
  } as never)
}

describe('createStorage', () => {
  beforeEach(() => {
    vi.resetModules()
    vi.resetAllMocks()

    getServiceKeyUser.mockResolvedValue({})
    getPostgresConnection.mockResolvedValue(connection)
    createAgent.mockReturnValue({ monitor: vi.fn() })
    createStorageBackend.mockReturnValue({})
  })

  // v1 `BaseEvent.onStart`/`onClose`: the worker-shared backend's lifecycle, now the free
  // functions `getOrCreateStorageBackend` / `closeStorageBackend` (the queue's `onStop`).
  it('recreates the shared backend while the previous backend is closing', async () => {
    const { closeStorageBackend, getOrCreateStorageBackend } = await import('./base')
    const { promise, resolve } = Promise.withResolvers<void>()
    const first = { close: vi.fn(() => promise) }
    const next = { close: vi.fn() }
    const closed = vi.fn()
    createStorageBackend.mockReturnValueOnce(first).mockReturnValue(next)

    getOrCreateStorageBackend()
    const closing = closeStorageBackend().then(closed)
    const repeated = closeStorageBackend()
    getOrCreateStorageBackend()
    await createStorageForTest()

    expect(first.close).toHaveBeenCalledOnce()
    expect(closed).not.toHaveBeenCalled()
    expect(constructStorage.mock.lastCall?.[0]).toBe(next)

    resolve()
    await closing
    // The repeated close found no backend to close: it never waited on the first one.
    await expect(repeated).resolves.toBeUndefined()
    getOrCreateStorageBackend()
    await createStorageForTest()

    expect(createStorageBackend).toHaveBeenCalledTimes(2)
    expect(constructStorage.mock.lastCall?.[0]).toBe(next)
    await closeStorageBackend()
    expect(next.close).toHaveBeenCalledOnce()
  })

  it('recreates the backend after closing throws', async () => {
    const { closeStorageBackend, getOrCreateStorageBackend } = await import('./base')
    const closeError = new Error('close failed')
    const first = {
      close: vi.fn(() => {
        throw closeError
      }),
    }
    const next = { close: vi.fn() }
    createStorageBackend.mockReturnValueOnce(first).mockReturnValue(next)

    getOrCreateStorageBackend()
    // v1's `onClose` threw synchronously; `closeStorageBackend` is async, so it rejects.
    await expect(closeStorageBackend()).rejects.toBe(closeError)
    getOrCreateStorageBackend()
    await createStorageForTest()

    expect(constructStorage.mock.lastCall?.[0]).toBe(next)
  })

  it('disposes the connection when database construction fails', async () => {
    const constructionError = new Error('database construction failed')
    constructDatabase.mockImplementation(() => {
      throw constructionError
    })

    await expect(createStorageForTest()).rejects.toBe(constructionError)

    expect(connection.dispose).toHaveBeenCalledOnce()
    expect(createStorageBackend).not.toHaveBeenCalled()
    expect(constructStorage).not.toHaveBeenCalled()
  })

  it('disposes the connection when backend construction fails', async () => {
    const constructionError = new Error('backend construction failed')
    createStorageBackend.mockImplementation(() => {
      throw constructionError
    })

    await expect(createStorageForTest()).rejects.toBe(constructionError)

    expect(connection.dispose).toHaveBeenCalledOnce()
    expect(constructDatabase).toHaveBeenCalledOnce()
    expect(constructStorage).not.toHaveBeenCalled()
  })

  it('disposes the connection when storage construction fails', async () => {
    const constructionError = new Error('storage construction failed')
    constructStorage.mockImplementation(() => {
      throw constructionError
    })

    await expect(createStorageForTest()).rejects.toBe(constructionError)

    expect(connection.dispose).toHaveBeenCalledOnce()
    expect(constructDatabase).toHaveBeenCalledOnce()
    expect(createStorageBackend).toHaveBeenCalledOnce()
  })
})

function webhookPayload(): LifecyclePayload {
  return {
    tenant: { ref: 'tenant-a', host: 'tenant-a.example.test' },
    bucketId: 'bucket-a',
    name: 'path/file.png',
    version: 'version-1',
    reqId: 'req-1',
    sbReqId: 'sb-req-1',
    bytesChanged: -42,
  }
}

// v1 `BaseEvent.sendWebhook`, now the free function the lifecycle events delegate to.
describe('sendWebhook', () => {
  async function send(applyTime = Date.now()) {
    const { sendWebhook } = await import('./webhooks/webhook')
    return sendWebhook('ObjectCreated:Put', 'v1', webhookPayload(), applyTime)
  }

  beforeEach(() => {
    vi.resetModules()
    vi.resetAllMocks()
    produce.mockResolvedValue(undefined)
  })

  it('logs the billing event synchronously, before the webhook is enqueued', async () => {
    await send()

    expect(logInfo).toHaveBeenCalledTimes(1)
    expect(logInfo).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'event',
        event: 'ObjectCreated:Put',
        objectPath: 'tenant-a/bucket-a/path/file.png',
        objectVersion: 'version-1',
        bytesChanged: -42,
        resources: ['/tenant-a/bucket-a/path/file.png'],
        tenantId: 'tenant-a',
        project: 'tenant-a',
        reqId: 'req-1',
        sbReqId: 'sb-req-1',
      }),
      '[Lifecycle]: ObjectCreated:Put tenant-a/bucket-a/path/file.png'
    )
    expect(logInfo.mock.calls[0][0]).not.toHaveProperty('jobId')
    expect(produce).toHaveBeenCalledTimes(1)
    const { Webhook } = await import('./webhooks/webhook')
    const [message] = produce.mock.calls[0]
    expect(message).toBeInstanceOf(Webhook)
    expect(message.data).toEqual(
      expect.objectContaining({
        tenant: webhookPayload().tenant,
        event: expect.objectContaining({
          type: 'ObjectCreated:Put',
          $version: 'v1',
          region: 'local',
          payload: webhookPayload(),
        }),
      })
    )

    const [logOrder] = logInfo.mock.invocationCallOrder
    const [sendOrder] = produce.mock.invocationCallOrder
    expect(logOrder).toBeLessThan(sendOrder)
  })

  it('still logs the billing event exactly once when enqueuing the webhook fails', async () => {
    produce.mockRejectedValue(new Error('queue unavailable'))

    await expect(send()).resolves.toBeUndefined()

    expect(logInfo).toHaveBeenCalledTimes(1)
  })

  it('still enqueues the webhook, without throwing, when logging the billing event fails', async () => {
    logInfo.mockImplementation(() => {
      throw new Error('serialization failed')
    })

    await expect(send()).resolves.toBeUndefined()

    expect(produce).toHaveBeenCalledTimes(1)
    expect(logError).toHaveBeenCalledWith(
      expect.objectContaining({
        error: expect.any(Error),
        tenantId: 'tenant-a',
        sbReqId: 'sb-req-1',
      }),
      'error logging lifecycle event: ObjectCreated:Put'
    )
  })

  it('passes the caller-provided applyTime through to the webhook event', async () => {
    const commitTime = 1700000000000

    await send(commitTime)

    const [[sentWebhook]] = produce.mock.calls
    expect(sentWebhook.data.event.applyTime).toBe(commitTime)
  })

  it('logs the caller-provided applyTime when enqueuing the webhook fails', async () => {
    produce.mockRejectedValue(new Error('queue unavailable'))
    const commitTime = 1700000000000

    await send(commitTime)

    expect(logError).toHaveBeenCalledWith(
      expect.objectContaining({
        event: expect.objectContaining({ applyTime: commitTime }),
      }),
      'error sending webhook: ObjectCreated:Put'
    )
  })
})
