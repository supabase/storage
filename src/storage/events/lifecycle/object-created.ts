import { BasePayload } from '@internal/queue'
import { ObjectMetadata } from '../../backend'
import { BaseEvent } from '../base-event'
import { ObjectRemovedEvent, ReplacedObjectSnapshot } from './object-removed'

type ObjectCreatedUploadType = 'standard' | 'resumable' | 's3'

interface ObjectCreatedEventBase extends BasePayload {
  name: string
  version: string
  bucketId: string
  metadata: ObjectMetadata
  uploadType: ObjectCreatedUploadType
  // Net bytes this event added/removed to storage
  bytesChanged: number
  // Set only when bytesChanged includes a row this write replaced in place
  replacedObject?: ReplacedObjectSnapshot
}

type ObjectCreatedUploadEvent = ObjectCreatedEventBase

export interface ObjectedCreatedMove extends ObjectCreatedEventBase {
  oldObject: Pick<ObjectRemovedEvent, 'name' | 'bucketId' | 'version' | 'metadata'>
}

abstract class ObjectCreated<T extends ObjectCreatedEventBase> extends BaseEvent<T> {
  protected static queueName = 'object:created'
}

export class ObjectCreatedPutEvent extends ObjectCreated<ObjectCreatedUploadEvent> {
  static eventName() {
    return `ObjectCreated:Put`
  }
}

export class ObjectCreatedPostEvent extends ObjectCreated<ObjectCreatedUploadEvent> {
  static eventName() {
    return `ObjectCreated:Post`
  }
}

export class ObjectCreatedCopyEvent extends ObjectCreated<ObjectCreatedEventBase> {
  static eventName() {
    return `ObjectCreated:Copy`
  }
}

// Intentionally extends BaseEvent directly (not ObjectCreated) so it does not
// inherit `queueName = 'object:created'`. The move flow only calls sendWebhook.
export class ObjectCreatedMove extends BaseEvent<ObjectedCreatedMove> {
  static eventName() {
    return `ObjectCreated:Move`
  }
}
