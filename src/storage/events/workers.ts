import type { HandlerFor } from '@supabase-labs/wave-core'
import { PurgeCdnCacheHandler } from './cdn/purge-cdn-cache'
import { DeleteIcebergResourcesHandler } from './iceberg/delete-iceberg-resources'
import { ReconcileIcebergCatalogHandler } from './iceberg/reconcile-catalog'
import { JwksCreateSigningSecretHandler } from './jwks/jwks-create-signing-secret'
import { JwksRollUrlSigningKeyHandler } from './jwks/jwks-roll-url-signing-key'
import { BucketCreatedHandler } from './lifecycle/bucket-created'
import { BucketDeletedHandler } from './lifecycle/bucket-deleted'
import { ResetMigrationsHandler } from './migrations/reset-migrations'
import { RunMigrationsHandler } from './migrations/run-migrations'
import { BackupObjectHandler } from './objects/backup-object'
import { ObjectAdminDeleteHandler } from './objects/object-admin-delete'
import { ObjectAdminDeleteAllBeforeHandler } from './objects/object-admin-delete-all-before'
import type { StorageTopics } from './topics'
import {
  MoveJobsToPgbossHandler,
  MoveJobsToPgqueHandler,
  MoveJobsV10ToV12Handler,
  MoveJobsV12ToV10Handler,
} from './upgrades/move-jobs'
import { SyncCatalogIdsHandler } from './upgrades/sync-catalog-ids'
import { WebhookHandler } from './webhooks/webhook'

/** Every registered worker (v1 `registerWorkers()`), constructed fresh per wave. Wave
 * defaults every shared single-message worker to the streaming flow; handlers declare
 * `flow` only to deviate (or, like RunMigrationsHandler, to pin a load-bearing default). */
export function buildHandlers(): ReadonlyArray<HandlerFor<StorageTopics>> {
  return [
    new WebhookHandler(),
    new PurgeCdnCacheHandler(),
    new ObjectAdminDeleteHandler(),
    new ObjectAdminDeleteAllBeforeHandler(),
    new BackupObjectHandler(),
    new RunMigrationsHandler(),
    new ResetMigrationsHandler(),
    new JwksCreateSigningSecretHandler(),
    new JwksRollUrlSigningKeyHandler(),
    new ReconcileIcebergCatalogHandler(),
    new DeleteIcebergResourcesHandler(),
    new SyncCatalogIdsHandler(),
    new BucketCreatedHandler(),
    new BucketDeletedHandler(),
    new MoveJobsToPgqueHandler(),
    new MoveJobsToPgbossHandler(),
    new MoveJobsV10ToV12Handler(),
    new MoveJobsV12ToV10Handler(),
  ]
}
