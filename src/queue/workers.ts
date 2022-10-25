import { Queue } from './queue'
import { Webhook, ObjectCreated } from './events'

export function registerWorkers() {
  Queue.register(ObjectCreated)
  Queue.register(Webhook)
}
