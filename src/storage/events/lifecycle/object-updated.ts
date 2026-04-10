import { BasePayload } from '@internal/queue'
import { ObjectMetadata } from '../../backend'
import { BaseEvent } from '../base-event'

interface ObjectUpdatedMetadataEvent extends BasePayload {
  name: string
  bucketId: string
  version: string
  metadata: ObjectMetadata
}

export class ObjectUpdatedMetadata extends BaseEvent<ObjectUpdatedMetadataEvent> {
  protected static queueName = 'object:updated'

  static eventName() {
    return `ObjectUpdated:Metadata`
  }
}
