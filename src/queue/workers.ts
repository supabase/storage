import { Queue } from './queue'
import { AdminDeleteObject, Webhook } from './events'

export function registerWorkers() {
  Queue.register(Webhook)
  Queue.register(AdminDeleteObject)
}
