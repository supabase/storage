import { vi } from 'vitest'

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
  webhookSend,
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
  webhookSend: vi.fn(),
}))

vi.mock('./lifecycle/webhook', () => ({
  Webhook: { send: webhookSend },
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
  const { BaseEvent } = await import('./base-event')

  class TestEvent extends BaseEvent<{ tenant: { ref: string; host: string } }> {
    static createStorageForTest() {
      return this.createStorage({
        tenant: { ref: 'tenant-a', host: 'tenant-a.example.test' },
      })
    }
  }

  return TestEvent.createStorageForTest()
}

describe('BaseEvent.createStorage', () => {
  beforeEach(() => {
    vi.resetModules()
    vi.resetAllMocks()

    getServiceKeyUser.mockResolvedValue({})
    getPostgresConnection.mockResolvedValue(connection)
    createAgent.mockReturnValue({ monitor: vi.fn() })
    createStorageBackend.mockReturnValue({})
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

function webhookPayload() {
  return {
    tenant: { ref: 'tenant-a', host: 'tenant-a.example.test' },
    bucketId: 'bucket-a',
    name: 'path/file.png',
    reqId: 'req-1',
    sbReqId: 'sb-req-1',
  }
}

async function loadTestEvent() {
  const { BaseEvent } = await import('./base-event')

  class TestEvent extends BaseEvent<ReturnType<typeof webhookPayload>> {
    static eventName() {
      return 'ObjectCreated:Put'
    }
  }

  return TestEvent
}

describe('BaseEvent.sendWebhook', () => {
  beforeEach(() => {
    vi.resetModules()
    vi.resetAllMocks()
    webhookSend.mockResolvedValue(undefined)
  })

  it('logs the billing event synchronously, before the webhook is enqueued', async () => {
    const TestEvent = await loadTestEvent()

    await TestEvent.sendWebhook(webhookPayload())

    expect(logInfo).toHaveBeenCalledTimes(1)
    expect(logInfo).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'event',
        event: 'ObjectCreated:Put',
        objectPath: 'tenant-a/bucket-a/path/file.png',
        resources: ['/tenant-a/bucket-a/path/file.png'],
        tenantId: 'tenant-a',
        project: 'tenant-a',
        reqId: 'req-1',
        sbReqId: 'sb-req-1',
      }),
      '[Lifecycle]: ObjectCreated:Put tenant-a/bucket-a/path/file.png'
    )
    expect(logInfo.mock.calls[0][0]).not.toHaveProperty('jobId')
    expect(webhookSend).toHaveBeenCalledTimes(1)

    const [logOrder] = logInfo.mock.invocationCallOrder
    const [sendOrder] = webhookSend.mock.invocationCallOrder
    expect(logOrder).toBeLessThan(sendOrder)
  })

  it('still logs the billing event exactly once when enqueuing the webhook fails', async () => {
    webhookSend.mockRejectedValue(new Error('queue unavailable'))
    const TestEvent = await loadTestEvent()

    await TestEvent.sendWebhook(webhookPayload())

    expect(logInfo).toHaveBeenCalledTimes(1)
  })

  it('still enqueues the webhook, without throwing, when logging the billing event fails', async () => {
    logInfo.mockImplementation(() => {
      throw new Error('serialization go boom')
    })
    const TestEvent = await loadTestEvent()

    await expect(TestEvent.sendWebhook(webhookPayload())).resolves.toBeUndefined()

    expect(webhookSend).toHaveBeenCalledTimes(1)
    expect(logError).toHaveBeenCalledWith(
      expect.objectContaining({
        error: expect.any(Error),
        tenantId: 'tenant-a',
        sbReqId: 'sb-req-1',
      }),
      'error logging lifecycle event: ObjectCreated:Put'
    )
  })
})
