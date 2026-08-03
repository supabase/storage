import type { PgBossTopicExtension } from '@supabase-labs/wave-adapter-pgboss'
import { defineTopics, type RetryPolicy, type TopicMapOf, topic } from '@supabase-labs/wave-core'
import { PurgeCdnCache } from './cdn/purge-cdn-cache'
import { DeleteIcebergResources } from './iceberg/delete-iceberg-resources'
import { ReconcileIcebergCatalog } from './iceberg/reconcile-catalog'
import { JwksCreateSigningSecret } from './jwks/jwks-create-signing-secret'
import { JwksRollUrlSigningKey } from './jwks/jwks-roll-url-signing-key'
import { BucketCreatedEvent } from './lifecycle/bucket-created'
import { BucketDeleted } from './lifecycle/bucket-deleted'
import { ResetMigrationsOnTenant } from './migrations/reset-migrations'
import { RunMigrationsOnTenants } from './migrations/run-migrations'
import { BackupObjectEvent } from './objects/backup-object'
import { ObjectAdminDelete } from './objects/object-admin-delete'
import { ObjectAdminDeleteAllBefore } from './objects/object-admin-delete-all-before'
import { SyncCatalogIds } from './upgrades/sync-catalog-ids'
import { Webhook } from './webhooks/webhook'

/**
 * Extends wave core's `CreateTopicOptions` with the `pgboss` namespace, ourselves — the
 * adapter deliberately does not do this merge on our behalf (importing it for its runtime
 * `pgboss()` function should have no side effect on ambient types), so this is the one place
 * WE own, and the one place to edit if we ever swap adapters.
 */
declare module '@supabase-labs/wave-core' {
  interface CreateTopicOptions {
    readonly pgboss?: PgBossTopicExtension
  }
}

/**
 * The topic names — v1 queue names with colons hyphenated (pg-boss v12 forbids `:` and
 * wave reserves `.` as its group separator). Webhook-facing `event.type` strings are an
 * external contract and are NOT affected — only internal queue naming changes.
 */
export const TOPICS = {
  webhooks: 'webhooks',
  objectAdminDelete: 'object-admin-delete',
  objectAdminDeleteAllBefore: 'object-admin-delete-all-before',
  backupObject: 'backup-object',
  runMigrations: 'tenants-migrations-v2',
  resetMigrations: 'tenants-migrations-reset-v2',
  jwksCreateSigningSecret: 'tenants-jwks-create-v2',
  jwksRollUrlSigningKey: 'tenants-jwks-roll-url-signing-key-v1',
  reconcileIcebergCatalog: 'reconcile-iceberg-catalog',
  deleteIcebergResources: 'delete-iceberg-resources',
  syncCatalogIds: 'sync-iceberg-catalog-ids',
  bucketCreated: 'bucket-created',
  bucketDeleted: 'bucket-deleted',
  purgeCdnCache: 'cdn-purge-cache',
} as const

export const deadLetterTopicOf = <T extends string>(topic: T): `${T}-dead-letter` =>
  `${topic}-dead-letter`

/** v1's global retry posture (retryLimit 20): worker queues that declared nothing. v1 used
 * exponential backoff here; wave's RetryPolicy carries a fixed delay — 5s chosen. */
export const defaultRetry = (topic: string): RetryPolicy => ({
  maxAttempts: 21,
  backoffMs: 5_000,
  deadLetter: deadLetterTopicOf(topic),
})

/** The system-event posture (v1 retryLimit 3, retryDelay 5): migrations, jwks, iceberg. */
export const systemRetry = (topic: string): RetryPolicy => ({
  maxAttempts: 4,
  backoffMs: 5_000,
  deadLetter: deadLetterTopicOf(topic),
})

/** v1 BackupObjectEvent: retryLimit 5, retryDelay 5. */
export const backupRetry = (topic: string): RetryPolicy => ({
  maxAttempts: 6,
  backoffMs: 5_000,
  deadLetter: deadLetterTopicOf(topic),
})

/** A dead-letter topic: v1 gave DLQs a 30-day window for inspection. `pgboss` is typed on plain
 * `CreateTopicOptions` via the declaration merge above — no wrapper, no cast needed to declare
 * or read it back. */
function deadLetterTopic() {
  return topic<unknown>({
    pgboss: { retentionSeconds: 30 * 86_400, deleteAfterSeconds: 30 * 86_400 },
  })
}

const TWO_HOURS = 7_200

/**
 * The storage topic registry — every v1 queue, with its policy/expiry mapped to v12
 * vocabulary. `exclusive` (v1 `exactly_once`) topics dedupe by each message class's
 * `idempotencyKey` (v1 `singletonKey`); v1's `singletonHours` window is the class's
 * `idempotencyTtlMs` (a per-message field, no longer topic config). Each topic's queue
 * middleware config (`allowSync`/`disableKeys`) lives on its bound message class (see
 * `storageEvent` in `base.ts`), not here — `topic()` only carries provisioning options.
 */
export const storageTopics = defineTopics({
  [TOPICS.webhooks]: topic(Webhook, { pgboss: { expireInSeconds: 30 } }),
  [TOPICS.objectAdminDelete]: topic(ObjectAdminDelete, { pgboss: { expireInSeconds: 30 } }),
  [TOPICS.objectAdminDeleteAllBefore]: topic(ObjectAdminDeleteAllBefore, {
    pgboss: { policy: 'singleton', expireInSeconds: 30 },
  }),
  [TOPICS.backupObject]: topic(BackupObjectEvent, { pgboss: { policy: 'exclusive' } }),
  [TOPICS.runMigrations]: topic(RunMigrationsOnTenants, {
    pgboss: { policy: 'exclusive', expireInSeconds: 600 },
  }),
  [TOPICS.resetMigrations]: topic(ResetMigrationsOnTenant, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
  }),
  [TOPICS.jwksCreateSigningSecret]: topic(JwksCreateSigningSecret, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
  }),
  [TOPICS.jwksRollUrlSigningKey]: topic(JwksRollUrlSigningKey, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
  }),
  [TOPICS.reconcileIcebergCatalog]: topic(ReconcileIcebergCatalog, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
  }),
  [TOPICS.deleteIcebergResources]: topic(DeleteIcebergResources, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
  }),
  [TOPICS.syncCatalogIds]: topic(SyncCatalogIds, {
    pgboss: { policy: 'exclusive', expireInSeconds: TWO_HOURS },
  }),
  [TOPICS.bucketCreated]: topic(BucketCreatedEvent),
  [TOPICS.bucketDeleted]: topic(BucketDeleted),
  [TOPICS.purgeCdnCache]: topic(PurgeCdnCache, { pgboss: { policy: 'exclusive' } }),

  // Dead-letter topics, declared so they carry the 30-day inspection window instead of the
  // adapter-wide defaults (wave would otherwise auto-provision them on subscribe).
  [deadLetterTopicOf(TOPICS.webhooks)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.objectAdminDelete)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.objectAdminDeleteAllBefore)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.backupObject)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.runMigrations)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.resetMigrations)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.jwksCreateSigningSecret)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.jwksRollUrlSigningKey)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.reconcileIcebergCatalog)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.deleteIcebergResources)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.syncCatalogIds)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.bucketCreated)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.bucketDeleted)]: deadLetterTopic(),
  [deadLetterTopicOf(TOPICS.purgeCdnCache)]: deadLetterTopic(),
})

/** The declared topic map — what wave's `HandlerFor`/`getWave` constrain on (the
 * `storageTopics` value itself is the `TopicRegistry` wrapping this map). */
export type StorageTopics = TopicMapOf<typeof storageTopics>
