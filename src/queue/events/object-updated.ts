import { BaseEvent, BasePayload } from './base-event'
import { ObjectMetadata } from '../../storage/backend'

interface ObjectUpdatedMetadataEvent extends BasePayload {
  name: string
  bucketId: string
  metadata: ObjectMetadata
}

export class ObjectUpdatedMetadata extends BaseEvent<ObjectUpdatedMetadataEvent> {
  protected static queueName = 'object:updated'

  static eventName() {
    return `ObjectUpdated:Metadata`
  }
}
