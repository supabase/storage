import { BaseEvent, BasePayload } from './base-event'
import { ObjectMetadata } from '../../storage/backend'
import { ObjectRemovedEvent } from './object-removed'
import { getTenantBackendProvider } from '../../database/tenant'

interface ObjectCreatedEvent extends BasePayload {
  name: string
  bucketId: string
  version: string
  provider?: string
  metadata: ObjectMetadata
}

abstract class ObjectCreated extends BaseEvent<ObjectCreatedEvent> {
  protected static queueName = 'object:created'

  static async beforeSend<T extends BaseEvent<ObjectCreatedEvent>>(
    payload: Omit<T['payload'], '$version'>
  ) {
    payload.provider = payload.provider || (await getTenantBackendProvider(payload.tenant.ref))
    return payload
  }
}

export class ObjectCreatedPutEvent extends ObjectCreated {
  static eventName() {
    return `ObjectCreated:Put`
  }
}

export class ObjectCreatedPostEvent extends ObjectCreated {
  static eventName() {
    return `ObjectCreated:Post`
  }
}

export class ObjectCreatedCopyEvent extends ObjectCreated {
  static eventName() {
    return `ObjectCreated:Copy`
  }
}

export interface ObjectedCreatedMove extends ObjectCreatedEvent {
  oldObject: Omit<ObjectRemovedEvent, 'tenant' | '$version'>
}

export class ObjectCreatedMove extends BaseEvent<ObjectedCreatedMove> {
  static eventName() {
    return `ObjectCreated:Move`
  }

  static async beforeSend<T extends BaseEvent<ObjectCreatedEvent>>(
    payload: Omit<T['payload'], '$version'>
  ) {
    payload.provider = payload.provider || (await getTenantBackendProvider(payload.tenant.ref))
    return payload
  }
}
