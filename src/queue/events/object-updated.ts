import { BaseEvent, BasePayload } from './base-event'
import { ObjectMetadata } from '../../storage/backend'
import { getTenantBackendProvider } from '../../database/tenant'

interface ObjectUpdatedMetadataEvent extends BasePayload {
  name: string
  bucketId: string
  version: string
  provider?: string
  metadata: ObjectMetadata
}

export class ObjectUpdatedMetadata extends BaseEvent<ObjectUpdatedMetadataEvent> {
  protected static queueName = 'object:updated'

  static eventName() {
    return `ObjectUpdated:Metadata`
  }

  static async beforeSend<T extends BaseEvent<ObjectUpdatedMetadataEvent>>(
    payload: Omit<T['payload'], '$version'>
  ) {
    payload.provider = payload.provider || (await getTenantBackendProvider(payload.tenant.ref))
    return payload
  }
}
