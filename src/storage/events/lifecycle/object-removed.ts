import { BasePayload } from '@internal/queue'
import { BaseEvent } from '../base-event'

/**
 * Snapshot of a row physically affected by a write/delete other than the one
 * the enclosing event otherwise describes (e.g. the null-version row a delete marker's insert reused)
 */
export interface ReplacedObjectSnapshot {
  version?: string | null
  metadata?: Record<string, unknown> | null
}

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

export class ObjectRemoved extends BaseEvent<ObjectRemovedEvent> {
  protected static queueName = 'object:deleted'

  static eventName() {
    return `ObjectRemoved:Delete`
  }
}

export class ObjectRemovedMove extends BaseEvent<ObjectRemovedEvent> {
  protected static queueName = 'object-deleted'

  static eventName() {
    return `ObjectRemoved:Move`
  }
}
