import { vi } from 'vitest'

const {
  connection,
  constructDatabase,
  constructStorage,
  createAgent,
  createStorageBackend,
  getPostgresConnection,
  getServiceKeyUser,
} = vi.hoisted(() => ({
  connection: { dispose: vi.fn() },
  constructDatabase: vi.fn(),
  constructStorage: vi.fn(),
  createAgent: vi.fn(),
  createStorageBackend: vi.fn(),
  getPostgresConnection: vi.fn(),
  getServiceKeyUser: vi.fn(),
}))

vi.mock('@internal/database', () => ({
  getPostgresConnection,
  getServiceKeyUser,
}))

vi.mock('@internal/http', () => ({
  createAgent,
}))

vi.mock('@internal/monitoring', () => ({
  logger: { error: vi.fn() },
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

describe('BaseEvent', () => {
  beforeEach(() => {
    vi.resetModules()
    vi.resetAllMocks()

    getServiceKeyUser.mockResolvedValue({})
    getPostgresConnection.mockResolvedValue(connection)
    createAgent.mockReturnValue({ monitor: vi.fn() })
    createStorageBackend.mockReturnValue({})
  })

  it('recreates the shared backend while the previous backend is closing', async () => {
    const { BaseEvent } = await import('./base-event')
    const { promise, resolve } = Promise.withResolvers<void>()
    const first = { close: vi.fn(() => promise) }
    const next = { close: vi.fn() }
    const closed = vi.fn()
    createStorageBackend.mockReturnValueOnce(first).mockReturnValue(next)

    BaseEvent.onStart()
    const closing = Promise.resolve(BaseEvent.onClose()).then(closed)
    BaseEvent.onClose()
    BaseEvent.onStart()
    await createStorageForTest()

    expect(first.close).toHaveBeenCalledOnce()
    expect(closed).not.toHaveBeenCalled()
    expect(constructStorage.mock.lastCall?.[0]).toBe(next)

    resolve()
    await closing
    BaseEvent.onStart()
    await createStorageForTest()

    expect(createStorageBackend).toHaveBeenCalledTimes(2)
    expect(constructStorage.mock.lastCall?.[0]).toBe(next)
    BaseEvent.onClose()
    expect(next.close).toHaveBeenCalledOnce()
  })

  it('recreates the backend after closing throws', async () => {
    const { BaseEvent } = await import('./base-event')
    const closeError = new Error('close failed')
    const first = {
      close: vi.fn(() => {
        throw closeError
      }),
    }
    const next = { close: vi.fn() }
    createStorageBackend.mockReturnValueOnce(first).mockReturnValue(next)

    BaseEvent.onStart()
    expect(() => BaseEvent.onClose()).toThrow(closeError)
    BaseEvent.onStart()
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
