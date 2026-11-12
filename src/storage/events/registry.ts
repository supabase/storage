import { defineQueues, type QueueMapOf, queue, type RetryPolicy } from '@internal/queue/manager'
import { PurgeCdnCache } from './cdn/purge-cdn-cache'
import { DeleteIcebergResources } from './iceberg/delete-iceberg-resources'
import { ReclaimIcebergShardSlots } from './iceberg/reclaim-shard-slots'
import { ReconcileIcebergCatalog } from './iceberg/reconcile-catalog'
import { JwksCreateSigningSecret } from './jwks/jwks-create-signing-secret'
import { JwksRollUrlSigningKey } from './jwks/jwks-roll-url-signing-key'
import { BucketCreatedEvent } from './lifecycle/bucket-created'
import { BucketDeleted } from './lifecycle/bucket-deleted'
import { ResetMigrationsOnTenant } from './migrations/reset-migrations'
import { MIGRATION_MAX_RUNTIME_SECONDS, RunMigrationsOnTenants } from './migrations/run-migrations'
import { BackupObjectEvent } from './objects/backup-object'
import { ObjectAdminDelete } from './objects/object-admin-delete'
import { ObjectAdminDeleteAllBefore } from './objects/object-admin-delete-all-before'
import { ObjectAdminDeleteBatch } from './objects/object-admin-delete-batch'
import { MoveJobsV10ToV12, MoveJobsV12ToV10 } from './upgrades/move-jobs'
import { SyncCatalogIds } from './upgrades/sync-catalog-ids'
import { Webhook } from './webhooks/webhook'

/**
 * The queue names: v1 queue names with colons hyphenated (pg-boss v12 forbids `:`).
 * Webhook-facing `event.type` strings are an external contract and are NOT affected, only
 * internal queue naming changes.
 */
export const QUEUES = {
  webhooks: 'webhooks',
  objectAdminDelete: 'object-admin-delete',
  objectAdminDeleteAllBefore: 'object-admin-delete-all-before',
  objectAdminDeleteBatch: 'object-admin-delete-batch',
  backupObject: 'backup-object',
  runMigrations: 'tenants-migrations-v2',
  resetMigrations: 'tenants-migrations-reset-v2',
  jwksCreateSigningSecret: 'tenants-jwks-create-v2',
  jwksRollUrlSigningKey: 'tenants-jwks-roll-url-signing-key-v1',
  reconcileIcebergCatalog: 'reconcile-iceberg-catalog',
  reclaimIcebergShardSlots: 'reclaim-iceberg-shard-slots',
  deleteIcebergResources: 'delete-iceberg-resources',
  syncCatalogIds: 'sync-iceberg-catalog-ids',
  bucketCreated: 'bucket-created',
  bucketDeleted: 'bucket-deleted',
  purgeCdnCache: 'cdn-purge-cache',
  moveJobsV10ToV12: 'move-jobs-v10-to-v12',
  moveJobsV12ToV10: 'move-jobs-v12-to-v10',
} as const

export const deadLetterQueueOf = <T extends string>(name: T): `${T}-dead-letter` =>
  `${name}-dead-letter`

/** v1's global retry posture (retryLimit 20): worker queues that declared nothing. v1 used
 * exponential backoff here; a RetryPolicy carries a fixed delay, 5s chosen. */
export const defaultRetry = (name: string): RetryPolicy => ({
  maxAttempts: 21,
  backoffMs: 5_000,
  deadLetter: deadLetterQueueOf(name),
})

/** The system-event posture (v1 retryLimit 3, retryDelay 5): migrations, jwks, iceberg. */
export const systemRetry = (name: string): RetryPolicy => ({
  maxAttempts: 4,
  backoffMs: 5_000,
  deadLetter: deadLetterQueueOf(name),
})

/** v1 BackupObjectEvent: retryLimit 5, retryDelay 5. */
export const backupRetry = (name: string): RetryPolicy => ({
  maxAttempts: 6,
  backoffMs: 5_000,
  deadLetter: deadLetterQueueOf(name),
})

/** A dead-letter queue: v1 gave DLQs a 30-day window for inspection. On SQS a FIFO queue's
 * dead-letter queue must be FIFO as well. */
function deadLetterQueue(options: { fifo?: boolean } = {}) {
  return queue<unknown>({
    pgboss: { retentionSeconds: 30 * 86_400, deleteAfterSeconds: 30 * 86_400 },
    ...(options.fifo ? { sqs: DEDUP } : {}),
  })
}

/** Queues whose producers rely on dedup opt into it per engine: pg-boss through its queue
 * policy, SQS by being a FIFO queue (a fixed five-minute window, whatever the message's
 * `idempotencyTtlMs`). On a standard SQS queue an idempotency key is ignored. */
const DEDUP = { fifo: true } as const

const TWO_HOURS = 7_200

/**
 * The storage queue registry: every v1 queue, with its policy/expiry mapped to pg-boss v12
 * vocabulary. `exclusive` (v1 `exactly_once`) queues dedupe by each message class's
 * `idempotencyKey` (v1 `singletonKey`) over its `idempotencyTtlMs` (v1 `singletonHours`).
 * Per-event middleware config (`allowSync`/`disableKeys`) lives on the bound message class
 * (see `storageEvent` in `base.ts`); `queue()` only carries provisioning options.
 */
export const storageQueues = defineQueues({
  [QUEUES.webhooks]: queue(Webhook, { pgboss: { expireInSeconds: 30 } }),
  [QUEUES.objectAdminDelete]: queue(ObjectAdminDelete, { pgboss: { expireInSeconds: 30 } }),
  [QUEUES.objectAdminDeleteAllBefore]: queue(ObjectAdminDeleteAllBefore, {
    pgboss: { policy: 'singleton', expireInSeconds: 30 },
    sqs: DEDUP,
  }),
  [QUEUES.objectAdminDeleteBatch]: queue(ObjectAdminDeleteBatch, {
    pgboss: { expireInSeconds: 30 },
  }),
  [QUEUES.backupObject]: queue(BackupObjectEvent, { pgboss: { policy: 'exclusive' }, sqs: DEDUP }),
  [QUEUES.runMigrations]: queue(RunMigrationsOnTenants, {
    // v1 carried expireInMinutes: 10 here, which expired any migration outliving it (pgboss
    // expiry is a hard cap from started_on — touch does not extend it) and let the retry ack
    // through the LockTimeout path while the real run continued unsupervised. Sized to the
    // handler's consumeTimeout instead; liveness failure is the fast detector (heartbeats,
    // see RunMigrationsHandler).
    pgboss: { policy: 'exclusive', expireInSeconds: MIGRATION_MAX_RUNTIME_SECONDS },
    sqs: DEDUP,
  }),
  [QUEUES.resetMigrations]: queue(ResetMigrationsOnTenant, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
    sqs: DEDUP,
  }),
  [QUEUES.jwksCreateSigningSecret]: queue(JwksCreateSigningSecret, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
    sqs: DEDUP,
  }),
  [QUEUES.jwksRollUrlSigningKey]: queue(JwksRollUrlSigningKey, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
    sqs: DEDUP,
  }),
  [QUEUES.reconcileIcebergCatalog]: queue(ReconcileIcebergCatalog, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
    sqs: DEDUP,
  }),
  [QUEUES.reclaimIcebergShardSlots]: queue(ReclaimIcebergShardSlots, {
    pgboss: { policy: 'exclusive', expireInSeconds: 15 * 60 },
    sqs: DEDUP,
  }),
  [QUEUES.deleteIcebergResources]: queue(DeleteIcebergResources, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
    sqs: DEDUP,
  }),
  [QUEUES.syncCatalogIds]: queue(SyncCatalogIds, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
    sqs: DEDUP,
  }),
  [QUEUES.bucketCreated]: queue(BucketCreatedEvent),
  [QUEUES.bucketDeleted]: queue(BucketDeleted),
  [QUEUES.purgeCdnCache]: queue(PurgeCdnCache, { pgboss: { policy: 'exclusive' }, sqs: DEDUP }),
  // pg-boss schema-generation drains (see upgrades/move-jobs.ts). No idempotency window on
  // purpose: a move must stay re-runnable, and concurrent drains split the backlog.
  [QUEUES.moveJobsV10ToV12]: queue(MoveJobsV10ToV12, { pgboss: { expireInSeconds: TWO_HOURS } }),
  [QUEUES.moveJobsV12ToV10]: queue(MoveJobsV12ToV10, { pgboss: { expireInSeconds: TWO_HOURS } }),

  // Dead-letter queues, declared so they carry the 30-day inspection window instead of the
  // adapter-wide defaults (they would otherwise be auto-provisioned on subscribe).
  [deadLetterQueueOf(QUEUES.webhooks)]: deadLetterQueue(),
  [deadLetterQueueOf(QUEUES.objectAdminDelete)]: deadLetterQueue(),
  [deadLetterQueueOf(QUEUES.objectAdminDeleteAllBefore)]: deadLetterQueue({ fifo: true }),
  [deadLetterQueueOf(QUEUES.objectAdminDeleteBatch)]: deadLetterQueue(),
  [deadLetterQueueOf(QUEUES.backupObject)]: deadLetterQueue({ fifo: true }),
  [deadLetterQueueOf(QUEUES.runMigrations)]: deadLetterQueue({ fifo: true }),
  [deadLetterQueueOf(QUEUES.resetMigrations)]: deadLetterQueue({ fifo: true }),
  [deadLetterQueueOf(QUEUES.jwksCreateSigningSecret)]: deadLetterQueue({ fifo: true }),
  [deadLetterQueueOf(QUEUES.jwksRollUrlSigningKey)]: deadLetterQueue({ fifo: true }),
  [deadLetterQueueOf(QUEUES.reconcileIcebergCatalog)]: deadLetterQueue({ fifo: true }),
  [deadLetterQueueOf(QUEUES.reclaimIcebergShardSlots)]: deadLetterQueue({ fifo: true }),
  [deadLetterQueueOf(QUEUES.deleteIcebergResources)]: deadLetterQueue({ fifo: true }),
  [deadLetterQueueOf(QUEUES.syncCatalogIds)]: deadLetterQueue({ fifo: true }),
  [deadLetterQueueOf(QUEUES.bucketCreated)]: deadLetterQueue(),
  [deadLetterQueueOf(QUEUES.bucketDeleted)]: deadLetterQueue(),
  [deadLetterQueueOf(QUEUES.purgeCdnCache)]: deadLetterQueue({ fifo: true }),
  [deadLetterQueueOf(QUEUES.moveJobsV10ToV12)]: deadLetterQueue(),
  [deadLetterQueueOf(QUEUES.moveJobsV12ToV10)]: deadLetterQueue(),
})

/** The declared queue map: what `HandlerFor`/`getQueue` constrain on. */
export type StorageQueues = QueueMapOf<typeof storageQueues>
