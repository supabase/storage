import { PubSubAdapter } from '../internal/pubsub'

declare const pubSub: PubSubAdapter

void pubSub.subscribe('tenant-update', (message) => {
  if (typeof message === 'string') {
    void message.toUpperCase()
  }
})

function _typecheckSubscriberMustNarrowPayload() {
  void pubSub.subscribe(
    'tenant-update',
    // @ts-expect-error pubsub payloads are unknown at the transport boundary
    (message) => message.toUpperCase()
  )
}
