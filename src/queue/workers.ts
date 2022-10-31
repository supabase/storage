import { Queue } from './queue'
import { Webhook } from './events'

export function registerWorkers() {
  Queue.register(Webhook)
}
