import { describe, expect, it, vi } from 'vitest'
import { AsyncAbortController } from '@internal/concurrency'

describe('AsyncAbortController', () => {
  it('reuses nextGroup when accessed repeatedly', () => {
    const controller = new AsyncAbortController()

    const firstGroup = controller.nextGroup
    const secondGroup = controller.nextGroup

    expect(secondGroup).toBe(firstGroup)
  })

  it('reuses nested nextGroup access at each level', () => {
    const controller = new AsyncAbortController()
    const firstGroup = controller.nextGroup

    const firstNestedGroup = controller.nextGroup.nextGroup
    const secondNestedGroup = controller.nextGroup.nextGroup

    expect(controller.nextGroup).toBe(firstGroup)
    expect(secondNestedGroup).toBe(firstNestedGroup)
    expect(firstNestedGroup).toBe(firstGroup.nextGroup)
  })

  it('aborts a nextGroup child only once even after repeated access', async () => {
    const controller = new AsyncAbortController()
    const childGroup = controller.nextGroup
    const abortSpy = vi.spyOn(childGroup, 'abortAsync').mockResolvedValue(undefined)

    void controller.nextGroup
    void controller.nextGroup

    await controller.abortAsync()

    expect(abortSpy).toHaveBeenCalledTimes(1)
  })

  it('waits for parent abort handlers before aborting nested groups', async () => {
    const controller = new AsyncAbortController()
    const childGroup = controller.nextGroup
    const grandchildGroup = childGroup.nextGroup
    const order: string[] = []
    let releaseRootAbort!: () => void
    const rootAbortDone = new Promise<void>((resolve) => {
      releaseRootAbort = resolve
    })

    controller.signal.addEventListener('abort', async () => {
      order.push('root:start')
      await rootAbortDone
      order.push('root:end')
    })

    childGroup.signal.addEventListener('abort', () => {
      order.push('child')
    })

    grandchildGroup.signal.addEventListener('abort', () => {
      order.push('grandchild')
    })

    const abortPromise = controller.abortAsync()

    await Promise.resolve()
    expect(order).toEqual(['root:start'])

    releaseRootAbort()
    await abortPromise

    expect(order).toEqual(['root:start', 'root:end', 'child', 'grandchild'])
  })
})
