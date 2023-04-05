import { BaseEvent, BasePayload } from './base-event'
import { ObjectMetadata } from '../../storage/backend'
import { ObjectRemovedEvent } from './object-removed'

interface ObjectCreatedEvent extends BasePayload {
  name: string
  bucketId: string
  metadata: ObjectMetadata
}

abstract class ObjectCreated extends BaseEvent<ObjectCreatedEvent> {
  protected static queueName = 'object:created'
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
}
