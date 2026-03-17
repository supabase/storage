import { BasePayload } from '@internal/queue'
import { ObjectMetadata } from '../../backend'
import type { Obj } from '../../schemas'
import { BaseEvent } from '../base-event'

interface ObjectCreatedEventBase extends BasePayload {
  name: string
  version: string
  bucketId: string
  metadata: ObjectMetadata
}

interface ObjectCreatedUploadEvent extends ObjectCreatedEventBase {
  uploadType: 'standard' | 'resumable' | 's3'
}

interface ObjectCreatedMoveSource {
  name: string
  bucketId: string
  version: Obj['version'] | null
  reqId?: string
}

interface ObjectCreatedMoveEvent extends ObjectCreatedEventBase {
  oldObject: ObjectCreatedMoveSource
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

export class ObjectCreatedMove extends ObjectCreated<ObjectCreatedMoveEvent> {
  static eventName() {
    return `ObjectCreated:Move`
  }
}
