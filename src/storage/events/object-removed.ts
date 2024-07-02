import { BasePayload } from '@internal/queue'
import { BaseEvent } from './base-event'
import { ObjectMetadata } from '@storage/backend'

export interface ObjectRemovedEvent extends BasePayload {
  name: string
  bucketId: string
  version: string
  metadata?: ObjectMetadata
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
