import { Queue } from '@internal/queue'
import { Webhook } from './lifecycle/webhook'
import { ObjectAdminDelete } from './lifecycle/object-admin-delete'
import { RunMigrationsOnTenants } from './migrations/run-migrations'
import { BackupObjectEvent } from './objects/backup-object'
import { ResetMigrationsOnTenant } from './migrations/reset-migrations'
import { JwksCreateSigningSecret } from './jwks/jwks-create-signing-secret'
import { UpgradePgBossV10 } from './pgboss/upgrade-v10'
import { ObjectAdminDeleteBatch } from './lifecycle/object-admin-delete-batch'

export function registerWorkers() {
  Queue.register(Webhook)
  Queue.register(ObjectAdminDelete)
  Queue.register(ObjectAdminDeleteBatch)
  Queue.register(RunMigrationsOnTenants)
  Queue.register(BackupObjectEvent)
  Queue.register(ResetMigrationsOnTenant)
  Queue.register(JwksCreateSigningSecret)
  Queue.register(UpgradePgBossV10)
}
