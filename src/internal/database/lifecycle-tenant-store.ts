import type { DatabaseExecutor } from './connection'

export interface LifecycleTenantClaim {
  tenantId: string
  claimId: string
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

  async claimDueTenants(limit: number, leaseMs: number): Promise<LifecycleTenantClaim[]> {
    assertPositiveInteger(limit, 'Lifecycle tenant claim limit')
    assertPositiveInteger(leaseMs, 'Lifecycle tenant claim lease')

    const result = await this.database.query<LifecycleTenantClaim>({
      text: `
        WITH unclaimed AS MATERIALIZED (
          SELECT tenant_id, next_dispatch_at AS due_at
          FROM lifecycle_tenants
          WHERE claim_until IS NULL
            AND next_dispatch_at <= statement_timestamp()
          ORDER BY next_dispatch_at, tenant_id
          FOR UPDATE SKIP LOCKED
          LIMIT $1
        ),
        expired AS MATERIALIZED (
          SELECT tenant_id, claim_until AS due_at
          FROM lifecycle_tenants
          WHERE claim_until < statement_timestamp()
          ORDER BY claim_until, tenant_id
          FOR UPDATE SKIP LOCKED
          LIMIT $1
        ),
        due AS MATERIALIZED (
          SELECT tenant_id
          FROM (
            SELECT tenant_id, due_at FROM unclaimed
            UNION ALL
            SELECT tenant_id, due_at FROM expired
          ) candidates
          ORDER BY due_at, tenant_id
          LIMIT $1
        )
        UPDATE lifecycle_tenants AS target
        SET claim_id = COALESCE(target.claim_id, gen_random_uuid()),
            claim_until = clock_timestamp() + $2::bigint * interval '1 millisecond',
            next_dispatch_at = 'infinity'::timestamptz,
            last_dispatched_at = clock_timestamp()
        FROM due
        WHERE target.tenant_id = due.tenant_id
        RETURNING
          target.tenant_id AS "tenantId",
          target.claim_id AS "claimId"
      `,
      values: [limit, leaseMs],
    })

    return result.rows
  }

  async completeTenantDispatch(
    tenantId: string,
    claimId: string,
    computedNextDispatchAt: string,
    health?: { maxFailureCount: number; oldestInFlightStartedAt: string | null }
  ): Promise<boolean> {
    assertTimestamp(computedNextDispatchAt, 'computedNextDispatchAt')
    const result = await this.database.query({
      text: `
        UPDATE lifecycle_tenants
        SET next_dispatch_at = LEAST(next_dispatch_at, $3::timestamptz),
            claim_id = NULL,
            claim_until = NULL,
            last_completed_at = clock_timestamp(),
            last_success_at = clock_timestamp(),
            failure_count = 0,
            last_error = NULL,
            max_shard_failure_count = CASE WHEN $4::boolean THEN $5::integer ELSE max_shard_failure_count END,
            oldest_in_flight_started_at = CASE WHEN $4::boolean THEN $6::timestamptz ELSE oldest_in_flight_started_at END,
            shard_status_observed_at = CASE WHEN $4::boolean THEN clock_timestamp() ELSE shard_status_observed_at END
        WHERE tenant_id = $1
          AND claim_id = $2::uuid
      `,
      values: [
        tenantId,
        claimId,
        computedNextDispatchAt,
        health !== undefined,
        health?.maxFailureCount ?? null,
        health?.oldestInFlightStartedAt ?? null,
      ],
    })
    return result.rowCount === 1
  }

  async failTenantDispatch(
    tenantId: string,
    claimId: string,
    error: Record<string, unknown>
  ): Promise<boolean> {
    const result = await this.database.query({
      text: `
        UPDATE lifecycle_tenants
        SET next_dispatch_at = LEAST(next_dispatch_at,
              clock_timestamp() + LEAST(21600, 300 * power(2, LEAST(failure_count, 7)))
                * interval '1 second'
            ),
            claim_id = NULL,
            claim_until = NULL,
            last_completed_at = clock_timestamp(),
            failure_count = failure_count + 1,
            last_error = $3::jsonb
        WHERE tenant_id = $1
          AND claim_id = $2::uuid
      `,
      values: [tenantId, claimId, JSON.stringify(error)],
    })
    return result.rowCount === 1
  }
}

function assertPositiveInteger(value: number, label: string) {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${label} must be positive`)
}

function assertTimestamp(value: string, label: string) {
  if (Number.isNaN(Date.parse(value))) throw new Error(`${label} must be a timestamp`)
}
