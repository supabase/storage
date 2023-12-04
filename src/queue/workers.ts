import { Queue } from './queue'
import { ObjectAdminDelete, Webhook } from './events'

export function registerWorkers() {
  Queue.register(Webhook)
  Queue.register(ObjectAdminDelete)
}
