import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import { queueConnectionConfig } from '@internal/queue'
import type { SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import { Pool } from 'pg'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'
import { QUEUES } from '../registry'

const { pgQueueSchemaV2 } = getConfig()

/**
 * Schema-generation moves: drain the pending backlog between the v1 fleet's `pgboss_v10`
 * schema (see `v1QueueMap`) and v2's `pgboss_v12` (v2's answer to v1's `MoveJobs`). Both
 * live in the SAME queue database, so each direction is batched raw SQL: one statement per
 * batch deletes from the source and writes the destination, so a batch moves atomically (a
 * crash can lose no message and duplicates none). Only queued jobs (`created`, `retry`) move:
 * an `active` job is running on a worker and stays where it is.
 *
 * - Queues are renamed (colons → v12 names) and the job data is wrapped/unwrapped: v1 job
 *   data is the raw payload, v12 wraps it in the pg-boss adapter's envelope with the v1
 *   `eventName()` as the `message-type` header.
 * - `start_after` (v1 delays and staggers survive) and `singleton_key` carry over verbatim.
 * - Retry budgets reset: a moved message arrives as a fresh job with its queue's full
 *   budget.
 * - Dead-letter queues stay put: each schema's DLQ remains inspectable in place.
 */
export type MoveJobsPayload = BasePayload

/** Move the v1 fleet's pgboss_v10 backlog into pgboss_v12 (the master → v2 deploy cutover). */
export class MoveJobsV10ToV12 extends storageEvent<MoveJobsPayload>({
  type: 'MoveJobsV10ToV12',
  // Never run a move inline on a produce failure: it needs the queue database.
  allowSync: false,
}) {}

/** Move pgboss_v12 backlog back into pgboss_v10 (the rollback to a master-era deploy).
 * Safe to run on the still-running v2 fleet just before rolling back: only queued jobs move.
 * A job a worker is running (`active`) holds no row lock, so it is left to finish or expire
 * where it is; a second run picks up whatever expired back to `retry`. */
export class MoveJobsV12ToV10 extends storageEvent<MoveJobsPayload>({
  type: 'MoveJobsV12ToV10',
  allowSync: false,
}) {}

export class MoveJobsV10ToV12Handler extends QueueHandler(MoveJobsV10ToV12) {
  override readonly options: SubscribeOptions = {
    prefetch: 1,
    parallelism: 1,
  }

  async handle(ctx: StorageJob<MoveJobsPayload>): Promise<void> {
    await withQueueDb(async (db) => {
      if (!(await v1SchemaExists(db))) return
      let total = 0
      for (const mapping of v1QueueMap()) {
        if (ctx.signal.aborted) break
        total += await moveV10QueueToV12(db, mapping, ctx)
      }
      logSchema.info(logger, `[MoveJobs][v10→v12] done: moved ${total} messages`, {
        type: 'queue',
      })
      await reportV1Leftovers(db)
    })
  }
}

export class MoveJobsV12ToV10Handler extends QueueHandler(MoveJobsV12ToV10) {
  override readonly options: SubscribeOptions = {
    prefetch: 1,
    parallelism: 1,
  }

  async handle(ctx: StorageJob<MoveJobsPayload>): Promise<void> {
    await withQueueDb(async (db) => {
      if (!(await v1SchemaExists(db))) return
      let total = 0
      for (const mapping of v1QueueMap()) {
        if (ctx.signal.aborted) break
        total += await moveV12QueueToV10(db, mapping, ctx)
      }
      logSchema.info(logger, `[MoveJobs][v12→v10] done: moved ${total} messages`, {
        type: 'queue',
      })
    })
  }
}

/** Rows moved per statement — each batch is one atomic delete+insert round trip. */
const MOVE_BATCH_SIZE = 5000

/** The v1 fleet's pg-boss schema (master's hardcoded `PG_BOSS_SCHEMA`). */
const PGBOSS_V1_SCHEMA = 'pgboss_v10'

interface V1QueueMapping {
  /** The v2 queue (pgboss_v12 queue name). */
  readonly queue: string
  /** The v1 queue name (colons were legal there; v12 forbids them). */
  readonly v1Queue: string
  /** The v1 `eventName()`: what v2 stamps as the `message-type` header, so a moved job is
   * dispatched to the right handler. */
  readonly type: string
}

/**
 * v1 queue ↔ v2 queue ↔ message-type, from master's event classes (`static queueName` /
 * `eventName()`). v1 queues with no v2 queue (the object:created/updated/deleted lifecycle
 * queues — webhook-wrapped in v2) are deliberately absent; `reportV1Leftovers` names
 * whatever stays behind. A function, not a module const: this module sits in a cycle with
 * `registry.ts`, so `QUEUES` may only be read from deferred bodies, never at init.
 */
const v1QueueMap = (): readonly V1QueueMapping[] => [
  { queue: QUEUES.webhooks, v1Queue: 'webhooks', type: 'Webhook' },
  { queue: QUEUES.objectAdminDelete, v1Queue: 'object:admin:delete', type: 'ObjectAdminDelete' },
  {
    queue: QUEUES.objectAdminDeleteAllBefore,
    v1Queue: 'object:admin:delete-all-before',
    type: 'ObjectAdminDeleteAllBefore',
  },
  {
    queue: QUEUES.objectAdminDeleteBatch,
    v1Queue: 'object:admin:delete-batch',
    type: 'ObjectAdminDeleteBatch',
  },
  { queue: QUEUES.backupObject, v1Queue: 'backup-object', type: 'BackupObjectEvent' },
  {
    queue: QUEUES.reclaimIcebergShardSlots,
    v1Queue: 'reclaim-iceberg-shard-slots',
    type: 'ReclaimIcebergShardSlots',
  },
  { queue: QUEUES.runMigrations, v1Queue: 'tenants-migrations-v2', type: 'RunMigrationsOnTenants' },
  {
    queue: QUEUES.resetMigrations,
    v1Queue: 'tenants-migrations-reset-v2',
    type: 'ResetMigrationsOnTenant',
  },
  {
    queue: QUEUES.jwksCreateSigningSecret,
    v1Queue: 'tenants-jwks-create-v2',
    type: 'JwksCreateSigningSecret',
  },
  {
    queue: QUEUES.jwksRollUrlSigningKey,
    v1Queue: 'tenants-jwks-roll-url-signing-key-v1',
    type: 'JwksRollUrlSigningKey',
  },
  {
    queue: QUEUES.reconcileIcebergCatalog,
    v1Queue: 'reconcile-iceberg-catalog',
    type: 'ReconcileIcebergCatalog',
  },
  {
    queue: QUEUES.deleteIcebergResources,
    v1Queue: 'delete-iceberg-resources',
    type: 'DeleteIcebergResources',
  },
  { queue: QUEUES.syncCatalogIds, v1Queue: 'sync-iceberg-catalog-ids', type: 'SyncCatalogIds' },
  { queue: QUEUES.bucketCreated, v1Queue: 'bucket:created', type: 'Bucket:Created' },
  { queue: QUEUES.bucketDeleted, v1Queue: 'bucket:deleted', type: 'Bucket:Deleted' },
  { queue: QUEUES.purgeCdnCache, v1Queue: 'cdn:purge-cache', type: 'PurgeCdnCache' },
]

/** The pg-boss schema is env-configurable and interpolated into SQL: same identifier rule
 * the pg-boss adapter enforces on it. */
function assertSqlIdentifier(name: string): string {
  if (!/^[A-Za-z0-9_.]+$/.test(name)) {
    throw new Error(`[MoveJobs] unsafe SQL identifier: "${name}"`)
  }
  return name
}

async function withQueueDb(fn: (db: Pool) => Promise<void>): Promise<void> {
  // Both schemas live on the queue database. Single-statement batches are
  // transaction-pooler-safe.
  const pool = new Pool({ connectionString: queueConnectionConfig().pooledUrl, max: 1 })
  // An unlistened pool 'error' (the client dropping outside a query) kills the process.
  pool.on('error', (error) => {
    logSchema.error(logger, '[MoveJobs] queue pool error', { type: 'queue', error })
  })
  try {
    await fn(pool)
  } finally {
    await pool.end()
  }
}

async function v1SchemaExists(db: Pool): Promise<boolean> {
  const probe = await db.query<{ found: string | null }>(`SELECT to_regclass($1) AS found`, [
    `${PGBOSS_V1_SCHEMA}.job`,
  ])
  if (!probe.rows[0]?.found) {
    logSchema.warning(
      logger,
      `[MoveJobs] ${PGBOSS_V1_SCHEMA}.job does not exist; nothing to move`,
      {
        type: 'queue',
      }
    )
    return false
  }
  return true
}

/**
 * One batch per statement, v10 → v12: delete pending v1 jobs and re-insert them as v12 jobs
 * under the renamed queue, wrapping the raw v1 payload in the pg-boss adapter's envelope
 * (`{__q:1, p, h:{'message-type': <v1 eventName>}}`) so type dispatch reaches the right
 * handler. Fidelity choices: `singleton_key`, `start_after` (keeps v1 retry delays and
 * the FAILED_STALE stagger), and `priority` carry over; retry counts reset and retry/expiry
 * posture comes from the DESTINATION queue's config; the v1
 * transport-only `scheduleAt` payload field is stripped.
 */
async function moveV10QueueToV12(
  db: Pool,
  mapping: V1QueueMapping,
  ctx: StorageJob<MoveJobsPayload>
): Promise<number> {
  const v2Schema = assertSqlIdentifier(pgQueueSchemaV2)
  const v1Job = `${PGBOSS_V1_SCHEMA}.job`

  // The insert joins on the destination queue row; without this guard a missing queue would
  // make that join empty and the statement would DELETE source rows while inserting nothing.
  const destination = await db.query(`SELECT 1 FROM ${v2Schema}.queue WHERE name = $1`, [
    mapping.queue,
  ])
  if (destination.rowCount === 0) {
    logSchema.warning(
      logger,
      `[MoveJobs][v10→v12] destination queue ${mapping.queue} is not provisioned; skipping`,
      { type: 'queue' }
    )
    return 0
  }

  let moved = 0
  for (;;) {
    if (ctx.signal.aborted) break
    const result = await db.query<{ deleted: number; inserted: number }>({
      text: `
        WITH moved AS (
          DELETE FROM ${v1Job}
          WHERE id IN (
            SELECT id FROM ${v1Job}
            WHERE name = $1 AND state IN ('created', 'retry')
            LIMIT $4
            FOR UPDATE SKIP LOCKED
          )
          RETURNING id, data, singleton_key, start_after, priority
        ),
        inserted AS (
          INSERT INTO ${v2Schema}.job (
            id, name, data, state, policy, singleton_key, start_after, priority,
            retry_limit, retry_delay, retry_backoff, retry_delay_max,
            expire_seconds, deletion_seconds, keep_until, dead_letter, heartbeat_seconds
          )
          SELECT
            m.id, $2,
            jsonb_build_object(
              '__q', 1,
              'p', m.data - 'scheduleAt',
              'h', jsonb_build_object('message-type', $3::text)
            ),
            'created', q.policy, m.singleton_key, m.start_after, m.priority,
            q.retry_limit, q.retry_delay, q.retry_backoff, q.retry_delay_max,
            q.expire_seconds, q.deletion_seconds,
            now() + q.retention_seconds * interval '1 second',
            q.dead_letter, q.heartbeat_seconds
          FROM moved m
          JOIN ${v2Schema}.queue q ON q.name = $2
          ON CONFLICT DO NOTHING
          RETURNING 1
        )
        SELECT (SELECT count(*)::int FROM moved) AS deleted,
               (SELECT count(*)::int FROM inserted) AS inserted
      `,
      values: [mapping.v1Queue, mapping.queue, mapping.type, MOVE_BATCH_SIZE],
    })
    const batch = result.rows[0]?.deleted ?? 0
    if (batch > 0) {
      moved += result.rows[0]?.inserted ?? 0
      await ctx.heartbeat()
    }
    if (batch < MOVE_BATCH_SIZE) break
  }

  if (moved > 0) {
    logSchema.info(
      logger,
      `[MoveJobs][v10→v12] moved ${moved} ${mapping.v1Queue} → ${mapping.queue} messages`,
      { type: 'queue' }
    )
  }
  return moved
}

/** The reverse: unwrap the adapter's envelope back to the raw v1 payload under the v1 queue name.
 * Per-row retry/expiry travels FROM the v12 row (v10 keeps that config on the job row;
 * `expire_in` is an interval there); `policy` comes from the v10 queue row. */
async function moveV12QueueToV10(
  db: Pool,
  mapping: V1QueueMapping,
  ctx: StorageJob<MoveJobsPayload>
): Promise<number> {
  const v2Schema = assertSqlIdentifier(pgQueueSchemaV2)
  const v1Job = `${PGBOSS_V1_SCHEMA}.job`

  const destination = await db.query(`SELECT 1 FROM ${PGBOSS_V1_SCHEMA}.queue WHERE name = $1`, [
    mapping.v1Queue,
  ])
  if (destination.rowCount === 0) {
    logSchema.warning(
      logger,
      `[MoveJobs][v12→v10] destination queue ${mapping.v1Queue} is not provisioned; skipping`,
      { type: 'queue' }
    )
    return 0
  }

  let moved = 0
  for (;;) {
    if (ctx.signal.aborted) break
    const result = await db.query<{ deleted: number; inserted: number }>({
      text: `
        WITH moved AS (
          DELETE FROM ${v2Schema}.job
          WHERE id IN (
            SELECT id FROM ${v2Schema}.job
            WHERE name = $2 AND state IN ('created', 'retry')
            LIMIT $3
            FOR UPDATE SKIP LOCKED
          )
          RETURNING id, data, singleton_key, start_after, priority,
                    retry_limit, retry_delay, retry_backoff, expire_seconds, keep_until
        ),
        inserted AS (
          INSERT INTO ${v1Job} (
            id, name, data, state, singleton_key, start_after, priority,
            retry_limit, retry_delay, retry_backoff, expire_in, keep_until, policy
          )
          SELECT
            m.id, $1,
            COALESCE(m.data->'p', m.data),
            'created', m.singleton_key, m.start_after, m.priority,
            m.retry_limit, m.retry_delay, m.retry_backoff,
            make_interval(secs => m.expire_seconds), m.keep_until,
            q.policy
          FROM moved m
          JOIN ${PGBOSS_V1_SCHEMA}.queue q ON q.name = $1
          ON CONFLICT DO NOTHING
          RETURNING 1
        )
        SELECT (SELECT count(*)::int FROM moved) AS deleted,
               (SELECT count(*)::int FROM inserted) AS inserted
      `,
      values: [mapping.v1Queue, mapping.queue, MOVE_BATCH_SIZE],
    })
    const batch = result.rows[0]?.deleted ?? 0
    if (batch > 0) {
      moved += result.rows[0]?.inserted ?? 0
      await ctx.heartbeat()
    }
    if (batch < MOVE_BATCH_SIZE) break
  }

  if (moved > 0) {
    logSchema.info(
      logger,
      `[MoveJobs][v12→v10] moved ${moved} ${mapping.queue} → ${mapping.v1Queue} messages`,
      { type: 'queue' }
    )
  }
  return moved
}

/** v1 queues with no v2 queue (dropped lifecycle queues, strays) are left in place — name
 * them so the operator knows what did not travel. */
async function reportV1Leftovers(db: Pool): Promise<void> {
  const leftovers = await db.query<{ name: string; n: number }>(`
    SELECT name, count(*)::int AS n
    FROM ${PGBOSS_V1_SCHEMA}.job
    WHERE state IN ('created', 'retry', 'active')
    GROUP BY name
    ORDER BY name
  `)
  if (leftovers.rows.length > 0) {
    logSchema.warning(logger, `[MoveJobs][v10→v12] unmapped v1 backlog left behind`, {
      type: 'queue',
      metadata: JSON.stringify(leftovers.rows),
    })
  }
}
