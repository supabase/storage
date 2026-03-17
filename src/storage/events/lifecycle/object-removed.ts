import { BasePayload } from '@internal/queue'
import type { Obj } from '../../schemas'
import { BaseEvent } from '../base-event'

export interface ObjectRemovedEvent extends BasePayload {
  name: string
  bucketId: string
  version: Obj['version'] | null
  metadata: Obj['metadata']
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
