import { Queue } from './queue'
import { ObjectAdminDelete, Webhook } from './events'
import { RunMigrationsEvent } from './events/run-migrations'

export function registerWorkers() {
  Queue.register(Webhook)
  Queue.register(ObjectAdminDelete)
  Queue.register(RunMigrationsEvent)
}
