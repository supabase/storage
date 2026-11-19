import type { DatabaseExecutor } from './connection'

export interface LifecycleTenantClaim {
  tenantId: string
  claimId: string
  claimUntil: Date
  reclaimed: boolean
}

export class LifecycleTenantStorePg {
  constructor(private readonly database: DatabaseExecutor) {}

  async wakeTenant(tenantId: string, signal?: AbortSignal): Promise<void> {
    await this.database.query(
      {
        text: `
        INSERT INTO lifecycle_tenants (
          tenant_id,
          next_dispatch_at
        ) VALUES (
          $1,
          clock_timestamp()
        )
        ON CONFLICT (tenant_id) DO UPDATE
        SET next_dispatch_at = LEAST(
              lifecycle_tenants.next_dispatch_at,
              EXCLUDED.next_dispatch_at
            )
      `,
        values: [tenantId],
      },
      { signal }
    )
  }

  async hasObjectVersioningFeature(tenantId: string): Promise<boolean> {
    const result = await this.database.query<{ enabled: boolean }>({
      text: `
        SELECT EXISTS (
          SELECT 1 FROM tenants WHERE tenants.id = $1 AND tenants.feature_object_versioning
        ) AS enabled
      `,
      values: [tenantId],
    })
    return result.rows[0].enabled
  }

  async claimDueTenants(
    limit: number,
    leaseMs: number,
    unclaimedLimit = limit,
    signal?: AbortSignal
  ): Promise<LifecycleTenantClaim[]> {
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
        -- An expired lease can still have a queued job. Keep its claim id and back off
        -- redispatches; the scheduler records a failure only if the re-send creates a new job.
        UPDATE lifecycle_tenants AS target
        SET claim_id = COALESCE(target.claim_id, gen_random_uuid()),
            claim_until = clock_timestamp() + CASE
              WHEN target.claim_until IS NULL THEN $2::bigint * interval '1 millisecond'
              ELSE GREATEST(
                $2::bigint * interval '1 millisecond',
                LEAST(900, 300 * power(2, LEAST(target.failure_count, 7))) * interval '1 second'
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

  // Must run in a transaction: the lock serializes claimers until it ends.
  async claimDueTenantsWithinLeaseLimit(
    maxLeased: number,
    leaseMs: number,
    signal?: AbortSignal
  ): Promise<LifecycleTenantClaim[]> {
    await this.database.query("SELECT pg_advisory_xact_lock(hashtext('lifecycle_tenants_claim'))", {
      signal,
    })
    const result = await this.database.query<{ leased: number }>(
      'SELECT count(*)::int AS leased FROM lifecycle_tenants WHERE claim_until IS NOT NULL',
      { signal }
    )
    // An expired lease may still have a queued job: it keeps its slot, and reclaiming it adds no job.
    return this.claimDueTenants(
      maxLeased,
      leaseMs,
      Math.max(0, maxLeased - result.rows[0].leased),
      signal
    )
  }

  async completeTenantDispatch(
    tenantId: string,
    claimId: string,
    computedNextDispatchAt: string,
    deadline: string,
    health?: { maxFailureCount: number; oldestInFlightStartedAt: string | null }
  ): Promise<boolean> {
    const result = await this.database.query({
      text: `
        WITH owned AS (
          SELECT tenant_id FROM lifecycle_tenants
          WHERE tenant_id = $1 AND claim_id = $2::uuid
          FOR UPDATE
        ), current AS (
          SELECT tenant_id FROM owned
          WHERE clock_timestamp() < $7::timestamptz
        )
        UPDATE lifecycle_tenants AS target
        SET next_dispatch_at = LEAST(next_dispatch_at, $3::timestamptz),
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
        computedNextDispatchAt,
        health !== undefined,
        health?.maxFailureCount ?? null,
        health?.oldestInFlightStartedAt ?? null,
        deadline,
      ],
    })
    return result.rowCount === 1
  }

  async releaseTenantDispatch(
    tenantId: string,
    claimId: string,
    nextDispatchAt: string,
    deadline: string
  ): Promise<boolean> {
    const result = await this.database.query({
      text: `
        WITH owned AS (
          SELECT tenant_id FROM lifecycle_tenants
          WHERE tenant_id = $1 AND claim_id = $2::uuid
          FOR UPDATE
        ), current AS (
          SELECT tenant_id FROM owned
          WHERE clock_timestamp() < $4::timestamptz
        )
        UPDATE lifecycle_tenants AS target
        SET next_dispatch_at = LEAST(next_dispatch_at, $3::timestamptz),
            claim_id = NULL,
            claim_until = NULL
        FROM current
        WHERE target.tenant_id = current.tenant_id
      `,
      values: [tenantId, claimId, nextDispatchAt, deadline],
    })
    return result.rowCount === 1
  }

  async recordExpiredTenantDispatch(tenantId: string, claimId: string): Promise<boolean> {
    const result = await this.database.query({
      text: `
        UPDATE lifecycle_tenants
        SET failure_count = failure_count + 1,
            last_error = jsonb_build_object(
              'code', 'tenant_dispatch_lease_expired',
              'message', 'Tenant dispatch lease expired before completion'
            )
        WHERE tenant_id = $1 AND claim_id = $2::uuid
      `,
      values: [tenantId, claimId],
    })
    return result.rowCount === 1
  }

  async failTenantDispatch(
    tenantId: string,
    claimId: string,
    error: Record<string, unknown>,
    deadline: string
  ): Promise<boolean> {
    const result = await this.database.query({
      text: `
        WITH owned AS (
          SELECT tenant_id FROM lifecycle_tenants
          WHERE tenant_id = $1 AND claim_id = $2::uuid
          FOR UPDATE
        ), current AS (
          SELECT tenant_id FROM owned
          WHERE clock_timestamp() < $4::timestamptz
        )
        UPDATE lifecycle_tenants AS target
        SET next_dispatch_at = LEAST(next_dispatch_at,
              clock_timestamp() + LEAST(21600, 300 * power(2, LEAST(failure_count, 7)))
                * interval '1 second'
            ),
            claim_id = NULL,
            claim_until = NULL,
            last_completed_at = clock_timestamp(),
            failure_count = failure_count + 1,
            last_error = $3::jsonb
        FROM current
        WHERE target.tenant_id = current.tenant_id
      `,
      values: [tenantId, claimId, JSON.stringify(error), deadline],
    })
    return result.rowCount === 1
  }
}
