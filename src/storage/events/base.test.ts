import type { BasePayload } from '@internal/queue'
import { getConfig } from '../../config'
import { DEDUP_TTL_1H, storageEvent } from './base'

interface TestPayload extends BasePayload {
  name: string
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

// The v1 `Event` static send/invoke stamped `$version` into a copy of the caller's payload;
// in v2 that stamping happens once, in the message constructor.
describe('storageEvent payload versioning', () => {
  class TestEvent extends storageEvent<TestPayload>({
    type: 'TestEvent',
    version: 'v-test',
  }) {}

  it('does not mutate the payload passed to the constructor', () => {
    const payload = createPayload()

    const message = new TestEvent(payload)

    expect(payload).toEqual(createPayload())
    expect(payload).not.toHaveProperty('$version')
    expect(payload).not.toHaveProperty('region')
    expect(message.data).not.toBe(payload)
  })

  it('stamps $version and region into the wire data', () => {
    const { region } = getConfig()

    const message = new TestEvent(createPayload())

    expect(message.data).toEqual({
      ...createPayload(),
      $version: 'v-test',
      region,
    })
    expect(message.type).toBe('TestEvent')
  })

  it('preserves an explicit $version already on the payload', () => {
    const message = new TestEvent(createPayload({ $version: 'v-explicit' }))

    expect(message.data.$version).toBe('v-explicit')
  })

  it('defaults the class version to v1', () => {
    class DefaultVersionEvent extends storageEvent<TestPayload>({ type: 'DefaultVersionEvent' }) {}

    expect(DefaultVersionEvent.version).toBe('v1')
    expect(new DefaultVersionEvent(createPayload()).data.$version).toBe('v1')
  })
})

describe('storageEvent class options', () => {
  it('derives idempotencyKey from the wire payload, with the explicit init override winning', () => {
    class KeyedEvent extends storageEvent<TestPayload>({
      type: 'KeyedEvent',
      idempotencyKey: (data) => `keyed_${data.tenant.ref}`,
    }) {}

    expect(new KeyedEvent(createPayload()).idempotencyKey).toBe('keyed_test-tenant')
    expect(
      new KeyedEvent(createPayload(), { idempotencyKey: 'explicit-key' }).idempotencyKey
    ).toBe('explicit-key')
  })

  it('applies the class idempotencyTtlMs default (v1 singletonHours), init override winning', () => {
    class WindowedEvent extends storageEvent<TestPayload>({
      type: 'WindowedEvent',
      idempotencyKey: (data) => data.tenant.ref,
      idempotencyTtlMs: DEDUP_TTL_1H,
    }) {}

    expect(new WindowedEvent(createPayload()).idempotencyTtlMs).toBe(DEDUP_TTL_1H)
    expect(
      new WindowedEvent(createPayload(), { idempotencyTtlMs: 1_000 }).idempotencyTtlMs
    ).toBe(1_000)
  })

  it('exposes the queue middleware config on the class with v1 defaults', () => {
    class DefaultsEvent extends storageEvent<TestPayload>({ type: 'DefaultsEvent' }) {}
    class GuardedEvent extends storageEvent<TestPayload>({
      type: 'GuardedEvent',
      allowSync: false,
      disableKeys: () => [],
    }) {}

    expect(DefaultsEvent.eventType).toBe('DefaultsEvent')
    expect(DefaultsEvent.allowSync).toBe(true)
    expect(DefaultsEvent.disableKeys).toBeUndefined()

    expect(GuardedEvent.allowSync).toBe(false)
    expect(GuardedEvent.disableKeys?.(new GuardedEvent(createPayload()).data)).toEqual([])
  })
})
