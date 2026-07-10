# Jobs client

`jobs-client.ts` is an operator CLI for the admin queue-overflow API. When the pg-boss
`pgboss_v10.job` table accumulates so many `created` jobs that fetching slows down (high CPU,
starved workers), it lets you inspect the backlog, archive a slice of it into
`pgboss_v10.job_overflow_backup`, and restore it later in self-pacing batches.

It talks HTTP to the admin API. The server resolves
the same database connection pg-boss uses (including `PG_QUEUE_CONNECTION_URL` when configured),
so the routes require `PG_QUEUE_ENABLE=true`. To keep the maintenance API available while stopping
workers in a process, set `PG_QUEUE_WORKERS_ENABLE=false` rather than disabling the queue.

All operations only ever touch jobs in state `created`: jobs no worker has picked up.

## Actions

| Command                | Endpoint                       | What it does                                                    |
| ---------------------- | ------------------------------ | --------------------------------------------------------------- |
| `npm run jobs:list`    | `GET /queue/overflow`          | Counts `created` jobs grouped by queue/event type or by tenant. |
| `npm run jobs:backup`  | `POST /queue/overflow/backup`  | Moves matching `created` jobs into the overflow backup table.   |
| `npm run jobs:restore` | `POST /queue/overflow/restore` | Moves jobs back in batches until the backup table is drained.   |

## Environment variables

| Variable                  | Default   | Applies to | Description                                                                                    |
| ------------------------- | --------- | ---------- | ---------------------------------------------------------------------------------------------- |
| `ADMIN_URL`               | required  | all        | Base URL of the admin API.                                                                     |
| `ADMIN_API_KEY`           | required  | all        | Admin API key (sent as `ApiKey` header).                                                       |
| `JOBS_SOURCE`             | `job`     | list       | `job` (live table) or `backup` (overflow backup table).                                        |
| `JOBS_GROUP_BY`           | `summary` | list       | `summary` (queue + event type) or `tenant`.                                                    |
| `JOBS_QUEUE_NAME`         | –         | all        | Exact pg-boss queue name filter.                                                               |
| `JOBS_EVENT_TYPES`        | –         | all        | Comma-separated event type filter (`data->event->>type`).                                      |
| `JOBS_TENANT_REFS`        | –         | all        | Comma-separated tenant ref filter (`data->tenant->>ref`).                                      |
| `JOBS_LIMIT`              | see below | all        | List: max groups (50). Backup: max jobs in one call (unbounded). Restore: batch size (10,000). |
| `JOBS_BACKUP_CONFIRM_ALL` | –         | backup     | Must be `true` to back up with no filter at all.                                               |
| `JOBS_MAX_PENDING`        | `50000`   | restore    | Pause restoring while the global live `created` backlog exceeds this.                          |
| `JOBS_SLEEP_MS`           | `1000`    | restore    | Sleep between batches; doubles up to 60s while over `JOBS_MAX_PENDING`.                        |

## Examples

```sh
# Queue/event summary of the live backlog (totalCount, groupCount, hasMore).
ADMIN_URL=https://storage-admin.example.com ADMIN_API_KEY=... npm run jobs:list

# Top tenants for one queue and event type.
ADMIN_URL=... ADMIN_API_KEY=... \
JOBS_GROUP_BY=tenant JOBS_QUEUE_NAME=webhooks JOBS_EVENT_TYPES=ObjectRemoved:Delete \
npm run jobs:list

# Archive every created webhooks job for two tenants.
ADMIN_URL=... ADMIN_API_KEY=... \
JOBS_QUEUE_NAME=webhooks JOBS_TENANT_REFS=tenant-a,tenant-b \
npm run jobs:backup

# Archive everything (explicit escape hatch required).
ADMIN_URL=... ADMIN_API_KEY=... JOBS_BACKUP_CONFIRM_ALL=true npm run jobs:backup

# Drain the backup table back into the live queue.
ADMIN_URL=... ADMIN_API_KEY=... JOBS_QUEUE_NAME=webhooks npm run jobs:restore

# Inspect what is still archived.
ADMIN_URL=... ADMIN_API_KEY=... JOBS_SOURCE=backup npm run jobs:list
```

## Semantics

- List responses report `sourceTableExists` for the selected source. The live job table is present
  when a live query succeeds; backup-source queries return `false` until the backup table exists.
- Backup needs a filter or `JOBS_BACKUP_CONFIRM_ALL=true`.
- Backup copies job columns/defaults and keeps only the `(name, id)` primary key. Existing backup
  tables have inherited partial unique indexes removed before the move, so singleton and
  exactly-once jobs can be archived again. `JOBS_LIMIT` caps the move.
- Restore uses batches of 10,000. It pauses while the live backlog exceeds `JOBS_MAX_PENDING`, with
  backoff capped at 60 seconds. The throttle uses the count-only `GET /queue/overflow/count`
  endpoint rather than the grouped analysis query.
- PostgreSQL restore uses `ON CONFLICT DO NOTHING`: the live job wins, conflicting archived rows
  are dropped, and `conflictCount` reports how many were skipped. Restore is not supported on
  OrioleDB.
- Mutations share an advisory lock and take a `SHARE ROW EXCLUSIVE` job-table lock while moving
  rows. A disconnected request stops waiting for transaction acquisition and aborts in-flight SQL.
  Since pg-pool cannot cancel its internal checkout waiter, that waiter can remain until a client
  becomes available; a late client is released without running `BEGIN`.
- Once mutation SQL completes, `COMMIT` is deliberately not canceled by a disconnect.
- Progress goes to stderr; the final JSON summary goes to stdout.
