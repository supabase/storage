import { type LifecycleEventLog, logger } from '@internal/monitoring'
import type { BasePayload } from '@internal/queue'

/** What every lifecycle notification carries: the object it concerns, the version it acted on
 * and the net bytes it added to (or removed from) storage. */
export type LifecyclePayload = BasePayload & {
  bucketId: string
  name: string
  version: string | null
  bytesChanged: number
}

/** One lifecycle event to notify about: its type, payload and when it took effect. */
export interface LifecycleWebhook {
  readonly eventType: string
  readonly version: string
  readonly payload: LifecyclePayload
  /** The ordering timestamp for the consuming side (the commit time). */
  readonly applyTime: number
}

/**
 * The lifecycle log line is written where the event happens (the producer), not when the
 * webhook is delivered: it must not depend on the queue, the webhook URL or delivery retries.
 */
export function logLifecycleEvent({ eventType, payload }: LifecycleWebhook): void {
  try {
    const path = `${payload.tenant.ref}/${payload.bucketId}/${payload.name}`
    const eventLog: LifecycleEventLog = {
      type: 'event',
      event: eventType,
      payload: JSON.stringify(payload),
      objectPath: path,
      objectVersion: payload.version,
      bytesChanged: payload.bytesChanged,
      resources: ['/' + path],
      tenantId: payload.tenant.ref,
      project: payload.tenant.ref,
      reqId: payload.reqId,
      sbReqId: payload.sbReqId,
    }
    logger.info(eventLog, `[Lifecycle]: ${eventType} ${path}`)
  } catch (e) {
    logger.error(
      { error: e, sbReqId: payload.sbReqId, tenantId: payload.tenant.ref },
      `error logging lifecycle event: ${eventType}`
    )
  }
}
