import { Queue } from './queue'
import { ObjectAdminDelete, Webhook, RunMigrationsOnTenants } from './events'

export function registerWorkers() {
  Queue.register(Webhook)
  Queue.register(ObjectAdminDelete)
  Queue.register(RunMigrationsOnTenants)
}
