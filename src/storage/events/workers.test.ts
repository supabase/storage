import { Queue } from '@internal/queue'
import * as config from '../../config'
import { LifecycleBucketRun, LifecycleTenantRun } from './lifecycle-rules'
import { registerWorkers } from './workers'

vi.mock('./base-event', () => ({ BaseEvent: class {} }))

test.each([false, true])('registers lifecycle consumers: paused=%s', (paused) => {
  const register = vi.spyOn(Queue, 'register').mockImplementation(() => {})
  vi.spyOn(config, 'getConfig').mockReturnValue({
    ...config.getConfig(),
    storageLifecyclePaused: paused,
  })

  registerWorkers()

  expect(register).toHaveBeenCalled()
  expect(register.mock.calls.some(([event]) => event === LifecycleTenantRun)).toBe(!paused)
  expect(register.mock.calls.some(([event]) => event === LifecycleBucketRun)).toBe(!paused)
})
