import { BasePayload } from '@internal/queue'
import { BaseEvent } from './base-event'
import { ObjectMetadata } from '../backend'

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
