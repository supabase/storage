import { vi } from 'vitest'

type EventModule = typeof import('./event')
type QueueModule = typeof import('./queue')

type TestPayload = {
  name: string
  tenant: {
    ref: string
    host: string
  }
  reqId?: string
  scheduleAt?: Date
}

async function loadQueueModules(opts?: { pgQueueEnable?: boolean }) {
  vi.resetModules()

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    pgQueueEnable: opts?.pgQueueEnable ?? false,
  })

  const eventModule = (await import('./event')) as EventModule
  const queueModule = (await import('./queue')) as QueueModule

  return { eventModule, queueModule }
}

function defineTestEvent(EventBase: EventModule['Event']) {
  return class TestEvent extends EventBase<TestPayload> {
    static readonly version = 'v-test'
    protected static queueName = 'test-event'
  }
}

function createPayload(overrides: Partial<TestPayload> = {}): TestPayload {
  return {
    name: 'test-object',
    tenant: {
      ref: 'test-tenant',
      host: 'localhost',
    },
    reqId: 'req-123',
    ...overrides,
  }
}

describe('Event payload versioning', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    vi.resetModules()
  })

  it('does not mutate payloads passed to static send', async () => {
    const { eventModule } = await loadQueueModules()
    const TestEvent = defineTestEvent(eventModule.Event)
    const payload = createPayload()

    vi.spyOn(TestEvent.prototype, 'send').mockImplementation(function (
      this: InstanceType<typeof TestEvent>
    ) {
      expect(this.payload.$version).toBe('v-test')
      return Promise.resolve('queued')
    })

    await TestEvent.send(payload)

    expect(payload).toEqual(createPayload())
    expect(payload).not.toHaveProperty('$version')
  })

  it('does not mutate payloads passed to static invoke', async () => {
    const { eventModule } = await loadQueueModules()
    const TestEvent = defineTestEvent(eventModule.Event)
    const payload = createPayload()

    vi.spyOn(TestEvent.prototype, 'invoke').mockImplementation(function (
      this: InstanceType<typeof TestEvent>
    ) {
      expect(this.payload.$version).toBe('v-test')
      return Promise.resolve(null)
    })

    await TestEvent.invoke(payload)

    expect(payload).toEqual(createPayload())
    expect(payload).not.toHaveProperty('$version')
  })

  it('does not mutate payloads passed to static invokeOrSend', async () => {
    const { eventModule } = await loadQueueModules()
    const TestEvent = defineTestEvent(eventModule.Event)
    const payload = createPayload()

    vi.spyOn(TestEvent.prototype, 'invokeOrSend').mockImplementation(function (
      this: InstanceType<typeof TestEvent>
    ) {
      expect(this.payload.$version).toBe('v-test')
      return Promise.resolve(null)
    })

    await TestEvent.invokeOrSend(payload)

    expect(payload).toEqual(createPayload())
    expect(payload).not.toHaveProperty('$version')
  })

  it('does not mutate message payloads when batching', async () => {
    const { eventModule, queueModule } = await loadQueueModules({ pgQueueEnable: true })
    const TestEvent = defineTestEvent(eventModule.Event)
    const payload = createPayload({ scheduleAt: new Date('2026-04-07T10:00:00.000Z') })
    const insert = vi.fn().mockResolvedValue('job-id')

    vi.spyOn(queueModule.Queue, 'getInstance').mockReturnValue({ insert } as unknown as ReturnType<
      typeof queueModule.Queue.getInstance
    >)

    const message = new TestEvent(payload)

    await TestEvent.batchSend([message])

    expect(payload).toEqual(createPayload({ scheduleAt: new Date('2026-04-07T10:00:00.000Z') }))
    expect(payload).not.toHaveProperty('$version')
    expect(insert).toHaveBeenCalledWith([
      expect.objectContaining({
        name: 'test-event',
        deadLetter: 'test-event-dead-letter',
        data: expect.objectContaining({
          $version: 'v-test',
          name: 'test-object',
          tenant: expect.objectContaining({
            ref: 'test-tenant',
          }),
        }),
        startAfter: new Date('2026-04-07T10:00:00.000Z'),
      }),
    ])
  })
})
