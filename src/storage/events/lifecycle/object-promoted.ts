import { BasePayload } from '@internal/queue'
import { BaseEvent } from '../base-event'

export interface ObjectPromotedEvent extends BasePayload {
  name: string
  bucketId: string
  version: string | null
  metadata?: Record<string, unknown> | null
  bytesChanged: number
}

export class ObjectPromoted extends BaseEvent<ObjectPromotedEvent> {
  protected static queueName = 'object:promoted'

  static eventName() {
    return `ObjectPromoted:Delete`
  }
}

export class ObjectPromotedMove extends BaseEvent<ObjectPromotedEvent> {
  protected static queueName = 'object-promoted'

  static eventName() {
    return `ObjectPromoted:Move`
  }
}
