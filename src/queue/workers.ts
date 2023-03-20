import { Queue } from './queue'
import { ObjectAdminDelete, UploadCompletedEvent, Webhook } from './events'

export function registerWorkers() {
  Queue.register(Webhook)
  Queue.register(ObjectAdminDelete)
  Queue.register(UploadCompletedEvent)
}
