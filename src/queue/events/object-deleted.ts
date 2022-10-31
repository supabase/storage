import { BaseEvent, BasePayload } from './base-event'

interface ObjectDeletedEvent extends BasePayload {
  name: string
  bucketId: string
}

export class ObjectDeleted extends BaseEvent<ObjectDeletedEvent> {
  protected static queueName = 'object-deleted'
}
