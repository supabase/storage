import type { DatabaseExecutor } from './connection'

export interface LifecycleTenantClaim {
  tenantId: string
  claimId: string
  claimUntil: Date
  reclaimed: boolean
}

export interface ClaimLifecycleTenantsInput {
  maxLeased: number
  leaseMs: number
  signal?: AbortSignal
}

export interface StartTenantDispatchInput {
  tenantId: string
  claimId: string
  deadline: string
  leaseMs: number
  reclaimed?: boolean
}

export interface CompleteTenantDispatchInput {
  tenantId: string
  claimId: string
  nextDispatchAt: string
  deadline: string
  health?: { maxFailureCount: number; oldestInFlightStartedAt: string | null }
  startedAt?: string
}

export interface FailTenantDispatchInput {
  tenantId: string
  claimId: string
  error: Record<string, unknown>
  deadline: string
  maxBackoffSeconds?: number
  signal?: AbortSignal
}

export interface ReleaseTenantDispatchInput {
  tenantId: string
  claimId: string
  deadline: string
  signal?: AbortSignal
}

// Reclaimed-run marker stored inside `last_error` (instead of a dedicated column):
// when a reclaimed tenant run starts (`startTenantDispatch(..., reclaimed = true)`), we tag
// `last_error.claimId = claim_id::text` so expiry handling can tell apart:
// - an expired claim whose re-sent job never started (still queued / queue lag -> re-send)
// - an expired claim whose re-sent job did start and stalled/crashed (-> evict into backoff)
// `releaseTenantDispatch(es)` strips this key on clean release. Keep all sites using these constants.
const RECLAIMED_CLAIM_ID_KEY = 'claimId'
const RECLAIMED_RUN_STARTED_SQL = `last_error->>'${RECLAIMED_CLAIM_ID_KEY}' = claim_id::text`
const RECLAIMED_RUN_NOT_STARTED_SQL = `last_error->>'${RECLAIMED_CLAIM_ID_KEY}' IS DISTINCT FROM claim_id::text`

const DEFAULT_FAILURE_BACKOFF_MAX_SECONDS = 21_600
const FAILURE_BACKOFF_BASE_SECONDS = 300
const FAILURE_BACKOFF_MAX_EXPONENT = 9

const LEASE_EXPIRED_ERROR_SQL = `jsonb_build_object(
  'code', 'tenant_dispatch_lease_expired',
  'message', 'Tenant dispatch lease expired before completion'
)`

function fencedOwnedClaimCte(deadlineParam: string, extraSelect?: string): string {
  return `
    WITH owned AS (
      SELECT tenant_id FROM lifecycle_tenants
      WHERE tenant_id = $1 AND claim_id = $2::uuid
      FOR UPDATE
    ), current AS (
      SELECT tenant_id${extraSelect ? `, ${extraSelect}` : ''}
      FROM owned
      WHERE clock_timestamp() < ${deadlineParam}::timestamptz
    )
  `
}

function failureNextDispatchAtSql(maxBackoffSecondsSql: string): string {
  return `LEAST(
    target.next_dispatch_at,
    clock_timestamp() + LEAST(${maxBackoffSecondsSql}, ${FAILURE_BACKOFF_BASE_SECONDS} * power(2, LEAST(target.failure_count, ${FAILURE_BACKOFF_MAX_EXPONENT})))
      * interval '1 second',
    CASE
      WHEN target.wake_floor_at > clock_timestamp() THEN target.wake_floor_at
      ELSE 'infinity'::timestamptz
    END
  )`
}

const RELEASE_TENANT_UPDATE_SQL = `
  UPDATE lifecycle_tenants AS target
  SET next_dispatch_at = LEAST(target.next_dispatch_at, clock_timestamp()),
      claim_id = NULL,
      claim_until = NULL,
      -- Strip the reclaimed-run marker on clean release while preserving any prior error.
      last_error = NULLIF(target.last_error - '${RECLAIMED_CLAIM_ID_KEY}', '{}'::jsonb)
  FROM current
  WHERE target.tenant_id = current.tenant_id
`

export class LifecycleTenantStorePg {
  constructor(private readonly database: DatabaseExecutor) {}

  async wakeTenant(tenantId: string, signal?: AbortSignal, delayMs = 0): Promise<void> {
    await this.database.query(
      {
        text: `
        WITH target_time AS (
          SELECT clock_timestamp() + $2::bigint * interval '1 millisecond' AS at
        )
        INSERT INTO lifecycle_tenants (
          tenant_id,
          next_dispatch_at,
          wake_floor_at
        )
        SELECT
          $1,
          target_time.at,
          CASE WHEN $2::bigint > 0 THEN target_time.at ELSE NULL END
        FROM target_time
        ON CONFLICT (tenant_id) DO UPDATE
        SET next_dispatch_at = LEAST(
              lifecycle_tenants.next_dispatch_at,
              EXCLUDED.next_dispatch_at
            ),
            wake_floor_at = CASE
              WHEN EXCLUDED.wake_floor_at IS NOT NULL
                THEN GREATEST(
                  COALESCE(lifecycle_tenants.wake_floor_at, '-infinity'::timestamptz),
                  EXCLUDED.wake_floor_at
                )
              ELSE lifecycle_tenants.wake_floor_at
            END
      `,
        values: [tenantId, delayMs],
      },
      { signal }
    )
  }

  async hasObjectVersioningFeature(tenantId: string, signal?: AbortSignal): Promise<boolean> {
    const result = await this.database.query<{ enabled: boolean }>(
      {
        text: `
        SELECT EXISTS (
          SELECT 1 FROM tenants WHERE tenants.id = $1 AND tenants.feature_object_versioning
        ) AS enabled
      `,
        values: [tenantId],
      },
      { signal }
    )
    return result.rows[0].enabled
  }

  private async claimDueTenants({
    limit,
    leaseMs,
    unclaimedLimit = limit,
    signal,
  }: {
    limit: number
    leaseMs: number
    unclaimedLimit?: number
    signal?: AbortSignal
  }): Promise<LifecycleTenantClaim[]> {
    const result = await this.database.query<LifecycleTenantClaim>(
      {
        text: `
        WITH unclaimed AS MATERIALIZED (
          SELECT tenant_id, next_dispatch_at AS due_at, false AS reclaimed
          FROM lifecycle_tenants
          WHERE claim_until IS NULL
            AND next_dispatch_at <= statement_timestamp()
          ORDER BY next_dispatch_at, tenant_id
          FOR UPDATE SKIP LOCKED
          LIMIT $3
        ),
        expired AS MATERIALIZED (
          SELECT tenant_id, claim_until AS due_at, true AS reclaimed
          FROM lifecycle_tenants
          WHERE claim_until < statement_timestamp()
            AND ${RECLAIMED_RUN_NOT_STARTED_SQL}
          ORDER BY claim_until, tenant_id
          FOR UPDATE SKIP LOCKED
          LIMIT $1
        ),
        due AS MATERIALIZED (
          SELECT tenant_id, reclaimed
          FROM (
            SELECT tenant_id, due_at, reclaimed FROM unclaimed
            UNION ALL
            SELECT tenant_id, due_at, reclaimed FROM expired
          ) candidates
          ORDER BY due_at, tenant_id
          LIMIT $1
        )
        -- An expired lease whose re-sent run has not started may still have a queued job during
        -- queue lag. Keep its claim id with a 5m lease; a re-sent run that starts records its claim
        -- id in last_error, so its expiry is evicted into backoff by claimDueTenantsWithinLeaseLimit.
        UPDATE lifecycle_tenants AS target
        SET claim_id = COALESCE(target.claim_id, gen_random_uuid()),
            claim_until = clock_timestamp() + CASE
              WHEN target.claim_until IS NULL THEN $2::bigint * interval '1 millisecond'
              ELSE GREATEST(
                $2::bigint * interval '1 millisecond',
                interval '300 seconds'
              )
            END,
            next_dispatch_at = CASE WHEN target.claim_until IS NULL
              THEN 'infinity'::timestamptz ELSE target.next_dispatch_at END,
            last_dispatched_at = clock_timestamp()
        FROM due
        WHERE target.tenant_id = due.tenant_id
        RETURNING
          target.tenant_id AS "tenantId",
          target.claim_id AS "claimId",
          target.claim_until AS "claimUntil",
          due.reclaimed AS "reclaimed"
      `,
        values: [limit, leaseMs, unclaimedLimit],
      },
      { signal }
    )

    return result.rows
  }

  // Must run in a transaction: skips the poll when another scheduler already holds the lock.
  async claimDueTenantsWithinLeaseLimit({
    maxLeased,
    leaseMs,
    signal,
  }: ClaimLifecycleTenantsInput): Promise<LifecycleTenantClaim[]> {
    const lock = await this.database.query<{ locked: boolean }>(
      "SELECT pg_try_advisory_xact_lock(hashtext('lifecycle_tenants_claim')) AS locked",
      {
        signal,
      }
    )
    if (!lock.rows[0]?.locked) return []
    // Evict expired claims whose re-sent run started so chronic timeouts or crashes
    // back off and release their lease slots instead of starving healthy tenants.
    await this.database.query(
      `
        UPDATE lifecycle_tenants AS target
        SET next_dispatch_at = ${failureNextDispatchAtSql(String(DEFAULT_FAILURE_BACKOFF_MAX_SECONDS))},
            claim_id = NULL,
            claim_until = NULL,
            last_completed_at = clock_timestamp(),
            failure_count = target.failure_count + 1,
            last_error = ${LEASE_EXPIRED_ERROR_SQL}
        FROM (
          SELECT tenant_id
          FROM lifecycle_tenants
          WHERE claim_until < statement_timestamp()
            AND ${RECLAIMED_RUN_STARTED_SQL}
          ORDER BY claim_until, tenant_id
          FOR UPDATE SKIP LOCKED
        ) AS expired
        WHERE target.tenant_id = expired.tenant_id
      `,
      { signal }
    )
    const result = await this.database.query<{ leased: number }>(
      'SELECT count(*)::int AS leased FROM lifecycle_tenants WHERE claim_until IS NOT NULL',
      { signal }
    )
    // Remaining expired leases stay pinned to bound in-flight claims during outages.
    return this.claimDueTenants({
      limit: maxLeased,
      leaseMs,
      unclaimedLimit: Math.max(0, maxLeased - result.rows[0].leased),
      signal,
    })
  }

  async startTenantDispatch({
    tenantId,
    claimId,
    deadline,
    leaseMs,
    reclaimed = false,
  }: StartTenantDispatchInput): Promise<
    { claimId: string; claimUntil: Date; startedAt: Date } | undefined
  > {
    const result = await this.database.query<{
      claimId: string
      claimUntil: Date
      startedAt: Date
    }>({
      text: `
        ${fencedOwnedClaimCte('$3', 'gen_random_uuid() AS next_claim_id, clock_timestamp() AS started_at')}
        UPDATE lifecycle_tenants AS target
        SET claim_id = current.next_claim_id,
            claim_until = CASE WHEN $5
              THEN GREATEST(target.claim_until, clock_timestamp() + $4::bigint * interval '1 millisecond')
              ELSE clock_timestamp() + $4::bigint * interval '1 millisecond' END,
            -- Tag last_error with the rotated claim id when a reclaimed run starts so expiry
            -- matches RECLAIMED_RUN_STARTED_SQL and evicts it into backoff instead of re-sending.
            last_error = CASE WHEN $5
              THEN COALESCE(target.last_error, '{}'::jsonb) || jsonb_build_object('${RECLAIMED_CLAIM_ID_KEY}', current.next_claim_id)
              ELSE last_error END
        FROM current WHERE target.tenant_id = current.tenant_id
        RETURNING
          target.claim_id AS "claimId",
          target.claim_until AS "claimUntil",
          current.started_at AS "startedAt"
      `,
      values: [tenantId, claimId, deadline, leaseMs, reclaimed],
    })
    return result.rows[0]
  }

  async completeTenantDispatch({
    tenantId,
    claimId,
    nextDispatchAt,
    deadline,
    health,
    startedAt,
  }: CompleteTenantDispatchInput): Promise<boolean> {
    const result = await this.database.query({
      text: `
        ${fencedOwnedClaimCte('$7')}
        UPDATE lifecycle_tenants AS target
        SET next_dispatch_at = LEAST(
              next_dispatch_at,
              $3::timestamptz,
              CASE
                WHEN wake_floor_at > COALESCE($8::timestamptz, clock_timestamp())
                  THEN wake_floor_at
                ELSE 'infinity'::timestamptz
              END
            ),
            wake_floor_at = CASE
              WHEN wake_floor_at > COALESCE($8::timestamptz, clock_timestamp())
                THEN wake_floor_at
              ELSE NULL
            END,
            claim_id = NULL,
            claim_until = NULL,
            last_completed_at = clock_timestamp(),
            last_success_at = clock_timestamp(),
            failure_count = 0,
            last_error = NULL,
            max_bucket_failure_count = CASE WHEN $4::boolean THEN $5::integer ELSE max_bucket_failure_count END,
            oldest_in_flight_started_at = CASE WHEN $4::boolean THEN $6::timestamptz ELSE oldest_in_flight_started_at END,
            bucket_status_observed_at = CASE WHEN $4::boolean THEN clock_timestamp() ELSE bucket_status_observed_at END
        FROM current
        WHERE target.tenant_id = current.tenant_id
      `,
      values: [
        tenantId,
        claimId,
        nextDispatchAt,
        health !== undefined,
        health?.maxFailureCount ?? null,
        health?.oldestInFlightStartedAt ?? null,
        deadline,
        startedAt ?? null,
      ],
    })
    return result.rowCount === 1
  }

  async failTenantDispatch({
    tenantId,
    claimId,
    error,
    deadline,
    maxBackoffSeconds = DEFAULT_FAILURE_BACKOFF_MAX_SECONDS,
    signal,
  }: FailTenantDispatchInput): Promise<boolean> {
    const result = await this.database.query(
      {
        text: `
        ${fencedOwnedClaimCte('$4')}
        UPDATE lifecycle_tenants AS target
        SET next_dispatch_at = ${failureNextDispatchAtSql('$5::integer')},
            claim_id = NULL,
            claim_until = NULL,
            last_completed_at = clock_timestamp(),
            failure_count = failure_count + 1,
            last_error = $3::jsonb
        FROM current
        WHERE target.tenant_id = current.tenant_id
      `,
        values: [tenantId, claimId, JSON.stringify(error), deadline, maxBackoffSeconds],
      },
      { signal }
    )
    return result.rowCount === 1
  }

  async releaseTenantDispatch({
    tenantId,
    claimId,
    deadline,
    signal,
  }: ReleaseTenantDispatchInput): Promise<boolean> {
    const result = await this.database.query(
      {
        text: `
        ${fencedOwnedClaimCte('$3')}
        ${RELEASE_TENANT_UPDATE_SQL}
      `,
        values: [tenantId, claimId, deadline],
      },
      { signal }
    )
    return result.rowCount === 1
  }

  async releaseTenantDispatches(
    claims: readonly { tenantId: string; claimId: string; claimUntil: Date }[],
    signal?: AbortSignal
  ): Promise<number> {
    const result = await this.database.query(
      {
        text: `
        WITH input AS (
          SELECT * FROM unnest($1::text[], $2::uuid[], $3::timestamptz[])
            AS input(tenant_id, claim_id, deadline)
        ), owned AS (
          SELECT target.tenant_id, input.deadline
          FROM lifecycle_tenants AS target
          JOIN input ON target.tenant_id = input.tenant_id AND target.claim_id = input.claim_id
          FOR UPDATE OF target SKIP LOCKED
        ), current AS (
          SELECT tenant_id FROM owned WHERE clock_timestamp() < deadline
        )
        ${RELEASE_TENANT_UPDATE_SQL}
      `,
        values: [
          claims.map((claim) => claim.tenantId),
          claims.map((claim) => claim.claimId),
          claims.map((claim) => claim.claimUntil.toISOString()),
        ],
      },
      { signal }
    )
    return result.rowCount ?? 0
  }
}
