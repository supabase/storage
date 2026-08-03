import type { BasePayload } from '@internal/queue'
import { ObjectMetadata } from '../../backend'
import { sendWebhook } from './webhook'

/**
 * The lifecycle notifications: pure webhook payloads — never queued on their own topic, only
 * wrapped into the `webhooks` topic with their `eventType` as the webhook `event.type`
 * (external contract, unchanged from v1: `ObjectCreated:Put`, `ObjectRemoved:Delete`, …).
 * The static `sendWebhook` keeps v1's call shape at every producer site.
 */
function webhookLifecycleEvent<T extends BasePayload & { bucketId: string; name: string }>(
  eventType: string
) {
  return class WebhookLifecycleEvent {
    static readonly eventType = eventType
    static readonly version = 'v1'

    static sendWebhook(payload: T): Promise<void> {
      return sendWebhook(this.eventType, this.version, payload)
    }
  }
}

type ObjectCreatedUploadType = 'standard' | 'resumable' | 's3'

export interface ObjectCreatedEventBase extends BasePayload {
  name: string
  version: string
  bucketId: string
  metadata: ObjectMetadata
  uploadType: ObjectCreatedUploadType
}

export type ObjectCreatedUploadEvent = ObjectCreatedEventBase

export interface ObjectRemovedEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
  metadata?: Record<string, unknown> | null
}

export interface ObjectedCreatedMove extends ObjectCreatedEventBase {
  oldObject: Omit<ObjectRemovedEvent, 'tenant' | '$version'>
}

export interface ObjectUpdatedMetadataEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
  metadata: ObjectMetadata
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

export class ObjectUpdatedMetadata extends webhookLifecycleEvent<ObjectUpdatedMetadataEvent>(
  'ObjectUpdated:Metadata'
) {}
