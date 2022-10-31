import { BaseEvent, BasePayload } from './base-event'
import { ObjectMetadata } from '../../storage/backend'

interface ObjectUpdatedEvent extends BasePayload {
  name: string
  bucketId: string
  metadata: ObjectMetadata
}

export class ObjectUpdated extends BaseEvent<ObjectUpdatedEvent> {
  protected static queueName = 'object-updated'
}
