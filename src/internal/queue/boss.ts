import type { QueueDefaults } from '@supabase-labs/wave-adapter-pgboss'
import { PgBoss } from 'pg-boss'
import { getConfig } from '../../config'

/**
 * The pg-boss v12 instance the queue runs on — caller-owned (shared with wave's pgboss
 * adapter AND any direct admin queries), on its own `pgQueueSchemaV2` schema so it never
 * touches the legacy v10-fork schema. Connection selection mirrors v1: the explicit queue
 * URL wins; multitenant deployments fall back to the multitenant DB (pooled URL disables
 * migrations — a transaction pooler cannot run them).
 */
export function createQueueBoss(opts: { enableWorkers: boolean }): PgBoss {
  const {
    isMultitenant,
    databaseURL,
    multitenantDatabasePoolUrl,
    multitenantDatabaseUrl,
    pgQueueConnectionURL,
    pgQueueMaxConnections,
    pgQueueSchemaV2,
    databaseApplicationName,
  } = getConfig()

  let url = pgQueueConnectionURL ?? databaseURL
  let migrate = true

  if (isMultitenant && !pgQueueConnectionURL) {
    if (!multitenantDatabaseUrl) {
      throw new Error('running storage in multi-tenant but DB_MULTITENANT_DATABASE_URL is not set')
    }
    url = multitenantDatabasePoolUrl || multitenantDatabaseUrl

    if (multitenantDatabasePoolUrl) {
      migrate = false
    }
  }

  return new PgBoss({
    connectionString: url,
    schema: pgQueueSchemaV2,
    migrate,
    max: pgQueueMaxConnections,
    application_name: databaseApplicationName,
    // Supervision follows the worker role, as in v1: producer-only nodes never run sweeps.
    // Wave forces cron scheduling off regardless; expiry/retention sweeps are pg-boss's own.
    supervise: opts.enableWorkers,
    schedule: false,
    maintenanceIntervalSeconds: 60 * 5,
  })
}

/**
 * The per-queue defaults wave stamps on every queue it creates — v1's global send/queue
 * posture mapped to v12's queue-level vocabulary. v12 has no archive table, so
 * `PG_QUEUE_ARCHIVE_COMPLETED_AFTER_SECONDS` has no equivalent: completed jobs are deleted
 * directly after the delete window.
 */
export function queueDefaults(): QueueDefaults {
  const { pgQueueDeleteAfterDays, pgQueueDeleteAfterHours, pgQueueRetentionDays } = getConfig()

  const deleteAfterHours = pgQueueDeleteAfterHours ?? (pgQueueDeleteAfterDays ?? 2) * 24
  return {
    expireInSeconds: 23 * 3600,
    retentionSeconds: (pgQueueRetentionDays ?? 2) * 86_400,
    deleteAfterSeconds: deleteAfterHours * 3600,
  }
}
