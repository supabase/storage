import type { BasePayload } from '@internal/queue'
import { ObjectMetadata } from '../../backend'
import { type LifecyclePayload, sendWebhook, sendWebhookBatch } from './webhook'

/**
 * The lifecycle notifications: pure webhook payloads — never queued on their own queue, only
 * wrapped into the `webhooks` queue with their `eventType` as the webhook `event.type`
 * (external contract, unchanged from v1: `ObjectCreated:Put`, `ObjectRemoved:Delete`, …).
 * The static `sendWebhook` keeps v1's call shape at every producer site; `sendWebhookBatch` is
 * its batch form, for the bulk deletes.
 */
function webhookLifecycleEvent<T extends LifecyclePayload>(eventType: string) {
  return class WebhookLifecycleEvent {
    static readonly eventType = eventType
    static readonly version = 'v1'

    static sendWebhook(payload: T, applyTime: number): Promise<void> {
      return sendWebhook(this.eventType, this.version, payload, applyTime)
    }

    /** `sendWebhook` for many events of this type, in one send to the queue. */
    static sendWebhookBatch(
      events: ReadonlyArray<{ readonly payload: T; readonly applyTime: number }>
    ): Promise<void> {
      return sendWebhookBatch(
        events.map(({ payload, applyTime }) => ({
          eventType: this.eventType,
          version: this.version,
          payload,
          applyTime,
        }))
      )
    }
  }
}

type ObjectCreatedUploadType = 'standard' | 'resumable' | 's3'

/**
 * Snapshot of a row physically affected by a write/delete other than the one
 * the enclosing event otherwise describes (e.g. the null-version row a delete marker's insert reused)
 */
export interface ReplacedObjectSnapshot {
  version?: string | null
  metadata?: Record<string, unknown> | null
}

/**
 * The `replacedObject` field for a lifecycle event payload, spread into the
 * webhook call. Empty when nothing was replaced in place.
 */
export function replacedObjectField(replaced: ReplacedObjectSnapshot | undefined): {
  replacedObject?: ReplacedObjectSnapshot
} {
  return replaced
    ? { replacedObject: { version: replaced.version, metadata: replaced.metadata } }
    : {}
}

export interface ObjectCreatedEventBase extends BasePayload {
  name: string
  version: string
  bucketId: string
  metadata: ObjectMetadata
  uploadType: ObjectCreatedUploadType
  // Net bytes this event added/removed to storage
  bytesChanged: number
  // Set only when bytesChanged includes a row this write replaced in place
  replacedObject?: ReplacedObjectSnapshot
}

export type ObjectCreatedUploadEvent = ObjectCreatedEventBase

export interface ObjectRemovedEvent extends BasePayload {
  name: string
  bucketId: string
  version: string | null
  metadata?: Record<string, unknown> | null
  // Net bytes this event added/removed to storage
  bytesChanged: number
  // Set only when bytesChanged includes a row this write replaced in place
  replacedObject?: ReplacedObjectSnapshot
}

export interface ObjectedCreatedMove extends ObjectCreatedEventBase {
  oldObject: Pick<ObjectRemovedEvent, 'name' | 'bucketId' | 'version' | 'metadata'>
}

export interface ObjectPromotedEvent extends BasePayload {
  name: string
  bucketId: string
  version: string | null
  metadata?: Record<string, unknown> | null
  bytesChanged: number
}

export class ObjectCreatedPutEvent extends webhookLifecycleEvent<ObjectCreatedUploadEvent>(
  'ObjectCreated:Put'
) {}

export class ObjectCreatedPostEvent extends webhookLifecycleEvent<ObjectCreatedUploadEvent>(
  'ObjectCreated:Post'
) {}

export class ObjectCreatedCopyEvent extends webhookLifecycleEvent<ObjectCreatedEventBase>(
  'ObjectCreated:Copy'
) {}

export class ObjectCreatedMove extends webhookLifecycleEvent<ObjectedCreatedMove>(
  'ObjectCreated:Move'
) {}

export class ObjectRemoved extends webhookLifecycleEvent<ObjectRemovedEvent>(
  'ObjectRemoved:Delete'
) {}

export class ObjectRemovedMove extends webhookLifecycleEvent<ObjectRemovedEvent>(
  'ObjectRemoved:Move'
) {}

export class ObjectPromoted extends webhookLifecycleEvent<ObjectPromotedEvent>(
  'ObjectPromoted:Delete'
) {}

export class ObjectPromotedMove extends webhookLifecycleEvent<ObjectPromotedEvent>(
  'ObjectPromoted:Move'
) {}
