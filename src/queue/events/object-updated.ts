import { BaseEvent, BasePayload } from './base-event'
import { ObjMetadata } from '../../storage/schemas'

interface ObjectUpdatedMetadataEvent extends BasePayload {
  name: string
  bucketId: string
  metadata: ObjMetadata
}

export class ObjectUpdatedMetadata extends BaseEvent<ObjectUpdatedMetadataEvent> {
  protected static queueName = 'object:updated'

  static eventName() {
    return `ObjectUpdated:Metadata`
  }
}
