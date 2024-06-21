import { BaseEvent, BasePayload } from './base-event'
import { ObjectRemovedEvent } from './object-removed'
import { ObjMetadata } from '../../storage/schemas'

interface ObjectCreatedEvent extends BasePayload {
  name: string
  bucketId: string
  metadata: ObjMetadata
  uploadType: 'standard' | 'resumable' | 's3'
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
