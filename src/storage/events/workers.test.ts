import type { AnyMessageClass, SubscribeOptions } from '@internal/queue/manager'
import { livenessOf } from '@internal/queue/manager/registry'
import { validateSubscribeOptions } from '@internal/queue/manager/validate'
import * as config from '../../config'
import { LifecycleBucketRunHandler, LifecycleTenantRunHandler } from './lifecycle-rules'
import { storageQueues } from './registry'
import { buildHandlers } from './workers'

describe('storage queue handlers', () => {
  const all = buildHandlers() as ReadonlyArray<{
    readonly messages: readonly AnyMessageClass[]
    readonly options?: SubscribeOptions
  }>
  const handlers = all.map((handler) => {
    const queue = storageQueues.classIndex.get(handler.messages[0]) as string
    return {
      name: handler.constructor.name,
      queue,
      options: handler.options ?? {},
      declared: storageQueues.get(queue)?.options,
    }
  })

  it.each(handlers)('$name bounds its attempt, within what its queue allows', (handler) => {
    const { options, declared, queue } = handler
    const consumeTimeout = options.consumeTimeout as number

    // No bound means a wedged handler holds its delivery until the queue takes it back and
    // hands it to another worker while this one is still running.
    expect(consumeTimeout).toBeGreaterThan(0)
    // A handler allowed to outlive the queue's liveness window keeps its delivery by heartbeat.
    if (consumeTimeout > livenessOf(declared)) {
      expect(options.heartbeatIntervalMs).toBeLessThan(livenessOf(declared))
    }
    // pg-boss expires an attempt at this cap whatever heartbeats say: never outlast it.
    const expireInSeconds = declared?.pgboss?.expireInSeconds
    if (expireInSeconds !== undefined) {
      expect(consumeTimeout).toBeLessThanOrEqual(expireInSeconds * 1000)
    }
    expect(() =>
      validateSubscribeOptions({ ...options, queue } as never, queue, declared)
    ).not.toThrow()
  })
})

test.each([false, true])('registers lifecycle consumers: paused=%s', (paused) => {
  vi.spyOn(config, 'getConfig').mockReturnValue({
    ...config.getConfig(),
    storageLifecyclePaused: paused,
  })

  const handlers = buildHandlers()

  expect(handlers.some((handler) => handler instanceof LifecycleTenantRunHandler)).toBe(!paused)
  expect(handlers.some((handler) => handler instanceof LifecycleBucketRunHandler)).toBe(!paused)
})
