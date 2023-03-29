import { Queue } from './queue'
import { ObjectAdminDelete, MultiPartUploadCompleted, Webhook } from './events'

export function registerWorkers() {
  Queue.register(Webhook)
  Queue.register(ObjectAdminDelete)
  Queue.register(MultiPartUploadCompleted)
}
