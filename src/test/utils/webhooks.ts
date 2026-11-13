import { type Mock, vi } from 'vitest'

interface LifecycleEvent<P> {
  sendWebhook(payload: P, applyTime: number): Promise<void>
  sendWebhookBatch(
    events: ReadonlyArray<{ readonly payload: P; readonly applyTime: number }>
  ): Promise<void>
}

export type WebhookSpy<P> = Mock<(payload: P, applyTime: number) => void> & {
  /** Stop the events from reaching the queue; they are still recorded. */
  stub(): WebhookSpy<P>
}

/**
 * Observe the webhooks an event class sends, whichever way it sends them: one at a time
 * (`sendWebhook`) or as a batch (`sendWebhookBatch`, the bulk deletes). The returned mock is
 * called once per event with `(payload, applyTime)`, so an assertion reads the same on both
 * paths. The events still go to the queue unless the spy is `stub()`bed.
 */
export function spyOnWebhooks<P>(Event: LifecycleEvent<P>): WebhookSpy<P> {
  const sendOne = Event.sendWebhook.bind(Event)
  const sendMany = Event.sendWebhookBatch.bind(Event)
  let stubbed = false

  const seen = vi.fn<(payload: P, applyTime: number) => void>()
  vi.spyOn(Event, 'sendWebhook').mockImplementation(async (payload, applyTime) => {
    seen(payload, applyTime)
    if (!stubbed) await sendOne(payload, applyTime)
  })
  vi.spyOn(Event, 'sendWebhookBatch').mockImplementation(async (events) => {
    for (const { payload, applyTime } of events) seen(payload, applyTime)
    if (!stubbed) await sendMany(events)
  })

  return Object.assign(seen, {
    stub() {
      stubbed = true
      return seen as WebhookSpy<P>
    },
  })
}
