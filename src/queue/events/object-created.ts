import { BaseEvent, BasePayload } from './base-event'
import { ObjectMetadata } from '../../storage/backend'

interface ObjectCreatedEvent extends BasePayload {
  name: string
  bucketId: string
  metadata: ObjectMetadata
}

export class ObjectCreated extends BaseEvent<ObjectCreatedEvent> {
  protected static queueName = 'object-created'
}