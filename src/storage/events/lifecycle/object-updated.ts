import { BasePayload } from '@internal/queue'
import { ObjectMetadata } from '../../backend'
import type { Obj } from '../../schemas'
import { BaseEvent } from '../base-event'

interface ObjectUpdatedMetadataEvent extends BasePayload {
  name: string
  bucketId: string
  version: Obj['version'] | null
  metadata: ObjectMetadata
}

export class ObjectUpdatedMetadata extends BaseEvent<ObjectUpdatedMetadataEvent> {
  protected static queueName = 'object:updated'

  static eventName() {
    return `ObjectUpdated:Metadata`
  }
}
