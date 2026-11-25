/**
 * Shared lifecycle scheduling, lease, and queue delivery timings.
 *
 * How these values relate:
 * - Tenant dispatch:
 *   - `LIFECYCLE_TENANT_CONSUME_TIMEOUT_MS` (150s) < `LIFECYCLE_TENANT_LEASE_MS` (180s) by 30s
 *     so a timed-out tenant run can still settle its central claim before the lease lapses.
 *   - The `lifecycle-tenant-run` queue's `livenessTimeoutMs` and pg-boss `expireInSeconds` both
 *     equal `LIFECYCLE_TENANT_LEASE_MS` (180s).
 *   - An unstarted expired lease is reclaimed with a floor of
 *     `LIFECYCLE_TENANT_RECLAIM_LEASE_FLOOR_SECONDS` (300s) so queue lag does not re-send faster
 *     than once per 5m. Failure backoff starts from the same
 *     `LIFECYCLE_TENANT_FAILURE_BACKOFF_BASE_SECONDS` (300s) up to 6h by default (1h on open-gate
 *     timeout, 24h on closed-gate failure).
 * - Bucket dispatch:
 *   - `LIFECYCLE_BUCKET_HEARTBEAT_INTERVAL_MS` (30s) << `LIFECYCLE_BUCKET_LIVENESS_TIMEOUT_MS`
 *     (180s) so live bucket workers renew liveness 6x per crash-takeover window.
 *   - `lifecycleBucketDeliveryMs(jobBudgetMs, claimLeaseMs)` sizes `consumeTimeout` to
 *     `budget + lease + 60s headroom` rounded up to minutes and capped at
 *     `LIFECYCLE_BUCKET_MAX_DELIVERY_MS` (23h59m), which also sets pg-boss `expireInSeconds`.
 *   - `LIFECYCLE_BUCKET_REDISPATCH_MS` (15m) bounds how long an unclaimed queued dispatch marker
 *     waits before a bucket becomes eligible for redispatch.
 * - Scheduler & release coordination:
 *   - `LIFECYCLE_RELEASE_TIMEOUT_MS` (5s) bounds best-effort claim releases on shutdown/error in
 *     the scheduler and the tenant/bucket handlers.
 *   - `LIFECYCLE_PRE_COMMIT_WAKE_TIMEOUT_MS` (2s) < `LIFECYCLE_POST_COMMIT_WAKE_TIMEOUT_MS` (5s)
 *     because the pre-commit wake holds the bucket `FOR UPDATE` lock inside the tenant transaction.
 *   - `LIFECYCLE_SCHEDULER_CLAIM_TIMEOUT_MS` (10s) < `LIFECYCLE_SCHEDULER_CLAIM_IDLE_TIMEOUT_MS`
 *     (30s) so stalled scheduler claim transactions abort before holding central DB connections.
 */

// Tenant dispatch lease, queue, and backoff timings
const LIFECYCLE_TENANT_EXPIRE_MINUTES = 3
export const LIFECYCLE_TENANT_LEASE_MS = LIFECYCLE_TENANT_EXPIRE_MINUTES * 60_000
export const LIFECYCLE_TENANT_CONSUME_TIMEOUT_MS = LIFECYCLE_TENANT_LEASE_MS - 30_000
export const LIFECYCLE_TENANT_RECLAIM_LEASE_FLOOR_SECONDS = 300
export const LIFECYCLE_TENANT_FAILURE_BACKOFF_BASE_SECONDS = 300
export const LIFECYCLE_TENANT_FAILURE_BACKOFF_MAX_SECONDS = 21_600
export const LIFECYCLE_TENANT_FAILURE_BACKOFF_MAX_EXPONENT = 9
export const LIFECYCLE_TENANT_TIMEOUT_BACKOFF_MAX_SECONDS = 60 * 60
export const LIFECYCLE_TENANT_CLOSED_GATE_FAILURE_BACKOFF_MAX_SECONDS = 24 * 60 * 60

// Bucket dispatch delivery, liveness, and heartbeat timings
export const LIFECYCLE_BUCKET_HEARTBEAT_INTERVAL_MS = 30_000
export const LIFECYCLE_BUCKET_LIVENESS_TIMEOUT_MS = 180_000
export const LIFECYCLE_BUCKET_REDISPATCH_MS = 15 * 60_000
export const LIFECYCLE_BUCKET_MAX_DELIVERY_MS = 24 * 60 * 60_000 - 60_000
const LIFECYCLE_BUCKET_DELIVERY_HEADROOM_MS = 60_000

export function lifecycleBucketDeliveryMs(jobBudgetMs: number, claimLeaseMs: number): number {
  return Math.min(
    LIFECYCLE_BUCKET_MAX_DELIVERY_MS,
    Math.ceil((jobBudgetMs + claimLeaseMs + LIFECYCLE_BUCKET_DELIVERY_HEADROOM_MS) / 60_000) *
      60_000
  )
}

// Scheduler poll, claim, and best-effort release timings
export const LIFECYCLE_SCHEDULER_POLL_INTERVAL_MS = 60_000
export const LIFECYCLE_SCHEDULER_IDLE_POLL_INTERVAL_MS = 600_000
export const LIFECYCLE_SCHEDULER_POLL_JITTER_MS = 5_000
export const LIFECYCLE_SCHEDULER_CLAIM_TIMEOUT_MS = 10_000
export const LIFECYCLE_SCHEDULER_CLAIM_IDLE_TIMEOUT_MS = 30_000
export const LIFECYCLE_RELEASE_TIMEOUT_MS = 5_000
export const LIFECYCLE_PRE_COMMIT_WAKE_TIMEOUT_MS = 2_000
export const LIFECYCLE_POST_COMMIT_WAKE_TIMEOUT_MS = 5_000
export const LIFECYCLE_PRE_COMMIT_WAKE_DELAY_MS = 5 * 60_000
