import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, WirePayload } from '@internal/queue'
import { queueConnectionConfig } from '@internal/queue'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { Pool } from 'pg'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'
import { systemRetry, TOPICS } from '../topics'

const { pgQueueSchemaV2 } = getConfig()

/**
 * Engine cutover tools: move the pending backlog of the INACTIVE engine into the running
 * one (v2's answer to v1's `MoveJobs`, which copied rows between pg-boss queues). Both
 * engines live in the SAME queue database, so each direction is batched raw SQL — one
 * statement per batch deletes from the source and writes the destination, so a batch moves
 * atomically (a crash can lose no message and duplicates none). A consumer/producer relay
 * over the adapters was tried first and deliberately removed — a generic wave `Relay` for
 * syncing topics across adapters is a possible second instance of that idea.
 *
 * Wire fidelity: both adapters store the same logical envelope — pg-boss `job.data` is
 * `{__wave:1, p, h, k?, a?, s?}`, pgque `ev_data` is `{h, p, a?}` with the partition key in
 * `ev_extra1` (`k:<key>` / `r:<n>` spread markers) — so the SQL translates mechanically.
 *
 * Operational semantics:
 * - pgboss → pgque moves states created/retry/active (v1 parity: `active` rows on a drained
 *   source are stranded leases). Each batch is followed by `pgque.wave_demand_tick` so a
 *   deep backlog lands in bounded windows (an untick'd bulk insert wedges the adapter at
 *   >65536 events/window). Idempotency windows are not re-registered on the destination —
 *   the source's singleton index already deduped what is being moved.
 * - pgque → pgboss derives the pending set from the fleet subscription's last COMPLETED
 *   tick (`ev_id` beyond its seq, or `ev_txid` not visible to its snapshot — the engine's
 *   own batching predicate), plus every parked copy in `retry_queue`. Work already settled
 *   inside the current unfinished window is re-moved (window-granular cursor, bounded by
 *   `ticker_max_count`) — duplicates, never losses. Original idempotency keys are recovered
 *   from `pgque.idem` by event id where the window is still live; otherwise a unique
 *   `moved:<ev_id>` key is synthesized so pg-boss's exclusive-policy index (which collides
 *   all NULL keys on '') cannot silently drop moved jobs.
 * - Retry budgets reset: a moved message arrives as a fresh job with its queue's full
 *   budget, due immediately (source retry backoff is not carried).
 * - Dead-letter topics stay put: each engine's DLQ remains inspectable in place.
 * - pgboss_v10 ↔ pgboss_v12 (the master-era fleet's schema, see `v1QueueMap`): same
 *   batched pattern plus queue renames (colons → v12 names) and envelope wrap/unwrap —
 *   v1 job data is the raw payload, v12 wraps it as `{__wave:1, p, h}` with the v1
 *   `eventName()` as the `message-type` header. These two carry `start_after` (v1 delays
 *   and staggers survive) and `singleton_key` verbatim.
 */
export type MoveJobsPayload = BasePayload

/** Move pg-boss (`pgQueueSchemaV2`) backlog into pgque. */
export class MoveJobsToPgque extends storageEvent<MoveJobsPayload>({
  type: 'MoveJobsToPgque',
  // Never run a move inline on a produce failure — it needs the queue database.
  allowSync: false,
}) {}

/** Move pgque backlog into pg-boss (the rollback direction). */
export class MoveJobsToPgboss extends storageEvent<MoveJobsPayload>({
  type: 'MoveJobsToPgboss',
  allowSync: false,
}) {}

/** Move the v1 fleet's pgboss_v10 backlog into pgboss_v12 (the master → v2 deploy cutover).
 * If the v2 fleet runs pgque, chain a `MoveJobsToPgque` afterwards. */
export class MoveJobsV10ToV12 extends storageEvent<MoveJobsPayload>({
  type: 'MoveJobsV10ToV12',
  allowSync: false,
}) {}

/** Move pgboss_v12 backlog back into pgboss_v10 (the rollback to a master-era deploy).
 * Run it on the still-running v2 fleet just before rolling back — SKIP LOCKED keeps it safe
 * to run while v2 workers are still consuming (a job is either moved or consumed, never
 * both). */
export class MoveJobsV12ToV10 extends storageEvent<MoveJobsPayload>({
  type: 'MoveJobsV12ToV10',
  allowSync: false,
}) {}

export class MoveJobsToPgqueHandler extends TopicHandler(MoveJobsToPgque) {
  override readonly options: SubscribeOptions = {
    prefetch: 1,
    parallelism: 1,
    retry: systemRetry(TOPICS.moveJobsToPgque),
  }

  async handle(ctx: JobContext<WirePayload<MoveJobsPayload>>): Promise<void> {
    await withQueueDb(async (db) => {
      let total = 0
      for (const topic of movableTopics()) {
        if (ctx.signal.aborted) break
        total += await movePgbossTopicToPgque(db, topic, ctx)
      }
      logSchema.info(logger, `[MoveJobs][pgboss→pgque] done: moved ${total} messages`, {
        type: 'queue',
      })
    })
  }
}

export class MoveJobsToPgbossHandler extends TopicHandler(MoveJobsToPgboss) {
  override readonly options: SubscribeOptions = {
    prefetch: 1,
    parallelism: 1,
    retry: systemRetry(TOPICS.moveJobsToPgboss),
  }

  async handle(ctx: JobContext<WirePayload<MoveJobsPayload>>): Promise<void> {
    await withQueueDb(async (db) => {
      let total = 0
      for (const topic of movableTopics()) {
        if (ctx.signal.aborted) break
        total += await movePgqueTopicToPgboss(db, topic, ctx)
      }
      logSchema.info(logger, `[MoveJobs][pgque→pgboss] done: moved ${total} messages`, {
        type: 'queue',
      })
    })
  }
}

export class MoveJobsV10ToV12Handler extends TopicHandler(MoveJobsV10ToV12) {
  override readonly options: SubscribeOptions = {
    prefetch: 1,
    parallelism: 1,
    retry: systemRetry(TOPICS.moveJobsV10ToV12),
  }

  async handle(ctx: JobContext<WirePayload<MoveJobsPayload>>): Promise<void> {
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

export class MoveJobsV12ToV10Handler extends TopicHandler(MoveJobsV12ToV10) {
  override readonly options: SubscribeOptions = {
    prefetch: 1,
    parallelism: 1,
    retry: systemRetry(TOPICS.moveJobsV12ToV10),
  }

  async handle(ctx: JobContext<WirePayload<MoveJobsPayload>>): Promise<void> {
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
  /** The v2 topic (pgboss_v12 queue name). */
  readonly topic: string
  /** The v1 queue name (colons were legal there; v12 forbids them). */
  readonly v1Queue: string
  /** The v1 `eventName()` — what v2 stamps as the `message-type` header, so wave dispatches
   * a moved job to the right handler. */
  readonly type: string
}

/**
 * v1 queue ↔ v2 topic ↔ message-type, from master's event classes (`static queueName` /
 * `eventName()`). v1 queues with no v2 topic (the object:created/updated/deleted lifecycle
 * queues — webhook-wrapped in v2) are deliberately absent; `reportV1Leftovers` names
 * whatever stays behind. A function, not a module const: this module sits in a cycle with
 * `topics.ts`, so `TOPICS` may only be read from deferred bodies, never at init.
 */
const v1QueueMap = (): readonly V1QueueMapping[] => [
  { topic: TOPICS.webhooks, v1Queue: 'webhooks', type: 'Webhook' },
  { topic: TOPICS.objectAdminDelete, v1Queue: 'object:admin:delete', type: 'ObjectAdminDelete' },
  {
    topic: TOPICS.objectAdminDeleteAllBefore,
    v1Queue: 'object:admin:delete-all-before',
    type: 'ObjectAdminDeleteAllBefore',
  },
  { topic: TOPICS.backupObject, v1Queue: 'backup-object', type: 'BackupObjectEvent' },
  { topic: TOPICS.runMigrations, v1Queue: 'tenants-migrations-v2', type: 'RunMigrationsOnTenants' },
  {
    topic: TOPICS.resetMigrations,
    v1Queue: 'tenants-migrations-reset-v2',
    type: 'ResetMigrationsOnTenant',
  },
  {
    topic: TOPICS.jwksCreateSigningSecret,
    v1Queue: 'tenants-jwks-create-v2',
    type: 'JwksCreateSigningSecret',
  },
  {
    topic: TOPICS.jwksRollUrlSigningKey,
    v1Queue: 'tenants-jwks-roll-url-signing-key-v1',
    type: 'JwksRollUrlSigningKey',
  },
  {
    topic: TOPICS.reconcileIcebergCatalog,
    v1Queue: 'reconcile-iceberg-catalog',
    type: 'ReconcileIcebergCatalog',
  },
  {
    topic: TOPICS.deleteIcebergResources,
    v1Queue: 'delete-iceberg-resources',
    type: 'DeleteIcebergResources',
  },
  { topic: TOPICS.syncCatalogIds, v1Queue: 'sync-iceberg-catalog-ids', type: 'SyncCatalogIds' },
  { topic: TOPICS.bucketCreated, v1Queue: 'bucket:created', type: 'Bucket:Created' },
  { topic: TOPICS.bucketDeleted, v1Queue: 'bucket:deleted', type: 'Bucket:Deleted' },
  { topic: TOPICS.purgeCdnCache, v1Queue: 'cdn:purge-cache', type: 'PurgeCdnCache' },
]

function movableTopics(): string[] {
  // The move topics themselves never move: a pending move event on the source would
  // re-produce a drain onto the engine it just came from.
  const moveTopics = new Set<string>([
    TOPICS.moveJobsToPgque,
    TOPICS.moveJobsToPgboss,
    TOPICS.moveJobsV10ToV12,
    TOPICS.moveJobsV12ToV10,
  ])
  return Object.values(TOPICS).filter((topic) => !moveTopics.has(topic))
}

/** The pg-boss schema is env-configurable and interpolated into SQL — same identifier rule
 * the pgboss adapter enforces on it. pgque relation names come from `pgque.queue` rows. */
function assertSqlIdentifier(name: string): string {
  if (!/^[A-Za-z0-9_.]+$/.test(name)) {
    throw new Error(`[MoveJobs] unsafe SQL identifier: "${name}"`)
  }
  return name
}

async function withQueueDb(fn: (db: Pool) => Promise<void>): Promise<void> {
  // Both engines live on the queue database. Single-statement batches are
  // transaction-pooler-safe, and `wave_demand_tick`'s advisory lock is xact-scoped.
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

/**
 * One batch per statement: delete pending pg-boss jobs, append them to pgque through the
 * engine's public `insert_event` — atomic per batch. The envelope moves `{__wave,p,h,k?,a?}`
 * → `{h,p,a?}` + `ev_extra1` key/spread marker, exactly as the pgque adapter's own append
 * writes it.
 */
async function movePgbossTopicToPgque(
  db: Pool,
  topic: string,
  ctx: JobContext<WirePayload<MoveJobsPayload>>
): Promise<number> {
  const jobTable = `${assertSqlIdentifier(pgQueueSchemaV2)}.job`
  let moved = 0

  for (;;) {
    if (ctx.signal.aborted) break
    const result = await db.query({
      text: `
        WITH moved AS (
          DELETE FROM ${jobTable}
          WHERE id IN (
            SELECT id FROM ${jobTable}
            WHERE name = $1 AND state IN ('created', 'retry', 'active')
            LIMIT $2
            FOR UPDATE SKIP LOCKED
          )
          RETURNING data
        ),
        numbered AS (
          SELECT data, row_number() OVER () AS rn FROM moved
        )
        SELECT pgque.insert_event(
          $1,
          'application/json',
          (
            jsonb_build_object(
              'h', COALESCE(n.data->'h', '{}'::jsonb),
              'p', COALESCE(n.data->'p', n.data)
            )
            || CASE WHEN n.data ? 'a' THEN jsonb_build_object('a', n.data->'a') ELSE '{}'::jsonb END
          )::text,
          CASE WHEN n.data ? 'k' THEN 'k:' || (n.data->>'k') ELSE 'r:' || n.rn::text END,
          NULL, NULL, NULL
        )
        FROM numbered n
      `,
      values: [topic, MOVE_BATCH_SIZE],
    })

    const batch = result.rowCount ?? 0
    if (batch > 0) {
      // Close the batch into a window so a deep backlog stays under the adapter's
      // 65536-events-per-window ceiling; serialized engine-side, cheap when redundant.
      await db.query('SELECT pgque.wave_demand_tick($1)', [topic])
      moved += batch
      await ctx.heartbeat()
    }
    if (batch < MOVE_BATCH_SIZE) break
  }

  if (moved > 0) {
    logSchema.info(logger, `[MoveJobs][pgboss→pgque] moved ${moved} ${topic} messages`, {
      type: 'queue',
    })
  }
  return moved
}

/** pgque event row → pg-boss job row: the shared tail of the retry-queue and event-table
 * statements. Expects a `moved` CTE (ev_id, ev_data, ev_extra1); copies the destination
 * queue's retry/expiry posture like v1's MoveJobs did, then RETURNS THE DELETED COUNT —
 * loop control must follow what left the source, because `ON CONFLICT DO NOTHING`
 * (deliberate dedup against already-queued singleton keys) makes the inserted count
 * undercount. */
function pgqueToPgbossInsertSql(schema: string): string {
  return `
    , inserted AS (
      INSERT INTO ${schema}.job (
        name, data, state, policy, singleton_key,
        retry_limit, retry_delay, retry_backoff, retry_delay_max,
        expire_seconds, deletion_seconds, keep_until, dead_letter, heartbeat_seconds
      )
      SELECT
        $1,
        (
          jsonb_build_object(
            '__wave', 1,
            'p', COALESCE(m.ev_data::jsonb->'p', m.ev_data::jsonb),
            'h', COALESCE(m.ev_data::jsonb->'h', '{}'::jsonb)
          )
          || CASE WHEN m.ev_data::jsonb ? 'a'
               THEN jsonb_build_object('a', m.ev_data::jsonb->'a') ELSE '{}'::jsonb END
          || CASE WHEN m.ev_extra1 LIKE 'k:%'
               THEN jsonb_build_object('k', substr(m.ev_extra1, 3)) ELSE '{}'::jsonb END
        ),
        'created',
        q.policy,
        COALESCE(i.idem_key, 'moved:' || m.ev_id::text),
        q.retry_limit, q.retry_delay, q.retry_backoff, q.retry_delay_max,
        q.expire_seconds, q.deletion_seconds,
        now() + q.retention_seconds * interval '1 second',
        q.dead_letter, q.heartbeat_seconds
      FROM moved m
      JOIN ${schema}.queue q ON q.name = $1
      LEFT JOIN pgque.idem i ON i.queue_id = $2 AND i.event_id = m.ev_id
      ON CONFLICT DO NOTHING
      RETURNING 1
    )
    SELECT (SELECT count(*)::int FROM moved) AS deleted,
           (SELECT count(*)::int FROM inserted) AS inserted
  `
}

/**
 * The pending set, loss-safe: parked retry copies, then every event the fleet subscription
 * has not durably finished — `ev_id` past the last completed tick's seq OR `ev_txid` not
 * visible to its snapshot (the engine's own batching predicate; the snapshot half also
 * catches raced inserts whose ids sit below the seq). No subscription / no tick ⇒ nothing
 * was ever consumed ⇒ everything moves. Moved rows are deleted, so re-runs converge.
 */
async function movePgqueTopicToPgboss(
  db: Pool,
  topic: string,
  ctx: JobContext<WirePayload<MoveJobsPayload>>
): Promise<number> {
  const schema = assertSqlIdentifier(pgQueueSchemaV2)

  const queueRow = await db.query<{
    queue_id: number
    queue_data_pfx: string
    queue_ntables: number
  }>('SELECT queue_id, queue_data_pfx, queue_ntables FROM pgque.queue WHERE queue_name = $1', [
    topic,
  ])
  const queue = queueRow.rows[0]
  if (!queue) {
    return 0
  }

  // The insert joins on the destination queue row; without this guard a missing queue would
  // make that join empty and the statement would DELETE source rows while inserting nothing.
  const destination = await db.query(`SELECT 1 FROM ${schema}.queue WHERE name = $1`, [topic])
  if (destination.rowCount === 0) {
    logSchema.warning(
      logger,
      `[MoveJobs][pgque→pgboss] destination queue ${topic} is not provisioned; skipping topic`,
      { type: 'queue' }
    )
    return 0
  }

  // Oldest position across the topic's fleet subscriptions (the adapter registers them as
  // `<topic>#<slot>/<n>`); oldest wins so a lagging slot can never lose work — only re-see it.
  const position = await db.query<{ seq: string | null; snap: string | null }>(
    `
      SELECT t.tick_event_seq::text AS seq, t.tick_snapshot::text AS snap
      FROM pgque.subscription s
      JOIN pgque.consumer c ON c.co_id = s.sub_consumer
      LEFT JOIN pgque.tick t ON t.tick_queue = s.sub_queue AND t.tick_id = s.sub_last_tick
      WHERE s.sub_queue = $1 AND (c.co_name = $2 OR c.co_name LIKE $2 || '#%')
      ORDER BY t.tick_event_seq ASC NULLS FIRST
      LIMIT 1
    `,
    [queue.queue_id, topic]
  )
  const seq = position.rows[0]?.seq ?? null
  const snap = position.rows[0]?.snap ?? null

  let moved = 0
  const drainStatement = async (text: string, values: unknown[]): Promise<void> => {
    for (;;) {
      if (ctx.signal.aborted) return
      const result = await db.query<{ deleted: number; inserted: number }>({ text, values })
      const batch = result.rows[0]?.deleted ?? 0
      if (batch > 0) {
        moved += result.rows[0]?.inserted ?? 0
        await ctx.heartbeat()
      }
      if (batch < MOVE_BATCH_SIZE) return
    }
  }

  // Parked retry copies first: the engine's retry sweep re-appends due copies into the
  // event tables, so anything it re-inserts mid-move is still caught by the pass below.
  await drainStatement(
    `
      WITH moved AS (
        DELETE FROM pgque.retry_queue
        WHERE ctid IN (
          SELECT ctid FROM pgque.retry_queue
          WHERE ev_queue = $2
          LIMIT $3
          FOR UPDATE SKIP LOCKED
        )
        RETURNING ev_id, ev_data, ev_extra1
      )
      ${pgqueToPgbossInsertSql(schema)}
    `,
    [topic, queue.queue_id, MOVE_BATCH_SIZE]
  )

  // The rotated event tables (<data_pfx>_0..n) all hold live history; ctid batching keeps
  // each statement bounded without relying on event-table indexes (pgq keeps none).
  for (let n = 0; n < queue.queue_ntables; n++) {
    if (ctx.signal.aborted) break
    const eventTable = assertSqlIdentifier(`${queue.queue_data_pfx}_${n}`)
    await drainStatement(
      `
        WITH moved AS (
          DELETE FROM ${eventTable}
          WHERE ctid IN (
            SELECT e.ctid FROM ${eventTable} e
            WHERE $4::bigint IS NULL
               OR e.ev_id > $4::bigint
               OR NOT pg_visible_in_snapshot(e.ev_txid, $5::pg_snapshot)
            LIMIT $3
            FOR UPDATE SKIP LOCKED
          )
          RETURNING ev_id, ev_data, ev_extra1
        )
        ${pgqueToPgbossInsertSql(schema)}
      `,
      [topic, queue.queue_id, MOVE_BATCH_SIZE, seq, snap]
    )
  }

  if (moved > 0) {
    logSchema.info(logger, `[MoveJobs][pgque→pgboss] moved ${moved} ${topic} messages`, {
      type: 'queue',
    })
  }
  return moved
}

/** Both v10 movers are no-ops (with a log line) when the old fleet's schema was never
 * installed or has already been dropped. */
async function v1SchemaExists(db: Pool): Promise<boolean> {
  const probe = await db.query<{ found: string | null }>(`SELECT to_regclass($1) AS found`, [
    `${PGBOSS_V1_SCHEMA}.job`,
  ])
  if (!probe.rows[0]?.found) {
    logSchema.warning(logger, `[MoveJobs] ${PGBOSS_V1_SCHEMA}.job does not exist; nothing to move`, {
      type: 'queue',
    })
    return false
  }
  return true
}

/**
 * One batch per statement, v10 → v12: delete pending v1 jobs and re-insert them as v12 jobs
 * under the renamed queue, wrapping the raw v1 payload in the wave envelope
 * (`{__wave:1, p, h:{'message-type': <v1 eventName>}}`) so wave's type dispatch reaches the
 * right handler. Fidelity choices: `singleton_key`, `start_after` (keeps v1 retry delays and
 * the FAILED_STALE stagger), and `priority` carry over; retry counts reset and retry/expiry
 * posture comes from the DESTINATION queue's config (like the pgque mover); the v1
 * transport-only `scheduleAt` payload field is stripped.
 */
async function moveV10QueueToV12(
  db: Pool,
  mapping: V1QueueMapping,
  ctx: JobContext<WirePayload<MoveJobsPayload>>
): Promise<number> {
  const v2Schema = assertSqlIdentifier(pgQueueSchemaV2)
  const v1Job = `${PGBOSS_V1_SCHEMA}.job`

  // The insert joins on the destination queue row; without this guard a missing queue would
  // make that join empty and the statement would DELETE source rows while inserting nothing.
  const destination = await db.query(`SELECT 1 FROM ${v2Schema}.queue WHERE name = $1`, [
    mapping.topic,
  ])
  if (destination.rowCount === 0) {
    logSchema.warning(
      logger,
      `[MoveJobs][v10→v12] destination queue ${mapping.topic} is not provisioned; skipping`,
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
            WHERE name = $1 AND state IN ('created', 'retry', 'active')
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
              '__wave', 1,
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
      values: [mapping.v1Queue, mapping.topic, mapping.type, MOVE_BATCH_SIZE],
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
      `[MoveJobs][v10→v12] moved ${moved} ${mapping.v1Queue} → ${mapping.topic} messages`,
      { type: 'queue' }
    )
  }
  return moved
}

/** The reverse: unwrap the wave envelope back to the raw v1 payload under the v1 queue name.
 * Per-row retry/expiry travels FROM the v12 row (v10 keeps that config on the job row;
 * `expire_in` is an interval there); `policy` comes from the v10 queue row. */
async function moveV12QueueToV10(
  db: Pool,
  mapping: V1QueueMapping,
  ctx: JobContext<WirePayload<MoveJobsPayload>>
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
            WHERE name = $2 AND state IN ('created', 'retry', 'active')
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
      values: [mapping.v1Queue, mapping.topic, MOVE_BATCH_SIZE],
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
      `[MoveJobs][v12→v10] moved ${moved} ${mapping.topic} → ${mapping.v1Queue} messages`,
      { type: 'queue' }
    )
  }
  return moved
}

/** v1 queues with no v2 topic (dropped lifecycle queues, strays) are left in place — name
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
